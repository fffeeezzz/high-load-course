package ru.quipy.common.utils

import okhttp3.Interceptor
import okhttp3.Response
import java.util.concurrent.TimeUnit


class Http3TimeoutInterceptor(private val timeout: () -> Long) : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val timeout = timeout().toInt()

        val withTimeoutsChain = chain
            .withConnectTimeout(timeout, TimeUnit.MILLISECONDS)
            .withReadTimeout(timeout, TimeUnit.MILLISECONDS)
            .withWriteTimeout(timeout, TimeUnit.MILLISECONDS)

        return withTimeoutsChain.proceed(chain.request())
    }
}