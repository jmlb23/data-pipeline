package com.github.jmlb23.data.kafka

import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.StartupEvent
import io.quarkus.runtime.annotations.QuarkusMain
import io.quarkus.websockets.next.WebSocketConnector
import io.quarkus.websockets.next.runtime.kotlin.ApplicationCoroutineScope
import io.smallrye.mutiny.coroutines.awaitSuspending
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.launch
import org.jboss.logging.Logger
import java.net.URI
import kotlin.coroutines.coroutineContext

@ApplicationScoped
class Application(
    private val connector: WebSocketConnector<BlueSkyClient>,
    private val logger: Logger,
    private val appScope: ApplicationCoroutineScope
) {
    fun onStartApp(@Observes event: StartupEvent) {
        logger.info("Initializing WebSocket connection...")

        runCatching {
            val blueSkyUri = URI.create("wss://jetstream.us-east.bsky.network")

            val client = connector.baseUri(blueSkyUri)

            appScope.launch {
                val result = client.connect().awaitSuspending()
            }

            logger.info("WebSocket connected. Awaiting termination signal...")

        }.fold({
            Quarkus.waitForExit()
        },{
            logger.error("something wrong happened ${it.message}")
            Quarkus.blockingExit()
        })

    }
}
