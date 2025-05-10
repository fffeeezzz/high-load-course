package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.dockerjava.api.command.AuthCmd.Exec
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.HdrHistogram.Histogram
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.Http3TimeoutInterceptor
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import kotlin.math.min

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val availableHTTPCodesToRetry = setOf(429, 500, 502, 503, 504)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private var avgRT: Duration = Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2)
    private var maxTrackableRT = requestAverageProcessingTime.toMillis() * 2
    private val hist = Histogram(requestAverageProcessingTime.toMillis(), maxTrackableRT, 2)

    private val client = OkHttpClient.Builder()
        .addInterceptor(Http3TimeoutInterceptor { avgRT.toMillis() })
        .build()

    private val rt = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private var semaphore = Semaphore(parallelRequests, true)

    private val pool = Executors.newFixedThreadPool(parallelRequests)

    private val cachedPool = Executors.newCachedThreadPool()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        try {
            cachedPool.submit {
                semaphore.acquire()

                if (expireByDeadline(deadline)) {
                    logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough parallel requests")
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            success = false,
                            now(),
                            transactionId,
                            reason = "Request would complete after deadline. No point in processing"
                        )
                    }

                    semaphore.release()
                    return@submit
                }

                rt.tickBlocking()

                if (expireByDeadline(deadline)) {
                    logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough rps tokens")
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            success = false,
                            now(),
                            transactionId,
                            reason = "Request would complete after deadline. No point in processing"
                        )
                    }

                    semaphore.release()
                    return@submit
                }

                try {
                    executeRequest(amount, transactionId, paymentId, deadline)
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error(
                                "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                e
                            )
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                            )

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                } finally {
                    semaphore.release()
                }
            }
        } catch (e: RejectedExecutionException) {
            logger.error("submit exception ...", e)
            performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun expireByDeadline(deadline: Long): Boolean {
        val expectedEnd = now() + avgRT.toMillis()

        return expectedEnd >= deadline
    }

    private fun executeRequest(
        amount: Int,
        transactionId: UUID,
        paymentId: UUID,
        deadline: Long,
        times: Int = 0,
        firstAttemptTime: Long = now()
    ) {
        times.inc()
        if (times == 3) {
            return
        }

        val startTime = now()

        val request = Request.Builder().run {
            url(
                "http://localhost:1234/external/process?" +
                        "serviceName=${serviceName}&" +
                        "accountName=${accountName}&" +
                        "transactionId=$transactionId&" +
                        "paymentId=$paymentId&" +
                        "amount=$amount&" +
                        "timeout=$avgRT"
            )
            post(emptyBody)
        }.build()

        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                hist.recordValue(now() - startTime)

                avgRT = Duration.ofMillis(minOf(hist.getValueAtPercentile(90.0), maxTrackableRT))

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                if ((availableHTTPCodesToRetry.contains(response.code) || !body.result) && !expireByDeadline(deadline)) {
                    val delta = now() - firstAttemptTime
                    if (delta <= 1000) {
                        Thread.sleep(1000 - delta)
                    }
                    executeRequest(amount, transactionId, paymentId, deadline, times)
                } else {
                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        } catch (e: Exception) {
            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: false, message: client exception")
            if (!expireByDeadline(deadline)) {
                val delta = now() - firstAttemptTime
                if (delta <= 1000) {
                    Thread.sleep(1000 - delta)
                }
                executeRequest(amount, transactionId, paymentId, deadline, times)
            } else {
                throw e
            }
        }
    }
}

public fun now() = System.currentTimeMillis()