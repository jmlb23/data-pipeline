package com.github.jmlb23.data.kafka_streams.utils

import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import kotlinx.serialization.json.Json

@ApplicationScoped
class JsonModule {
    @Produces
    fun produce(): Json =
        Json { ignoreUnknownKeys = true }

}
