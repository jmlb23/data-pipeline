package com.github.jmlb23.data.kafka

import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import io.quarkus.websockets.next.WebSocketConnector
import io.quarkus.websockets.next.runtime.kotlin.ApplicationCoroutineScope
import io.smallrye.mutiny.coroutines.awaitSuspending
import kotlinx.coroutines.launch
import org.jboss.logging.Logger
import java.net.URI
import kotlin.coroutines.coroutineContext

@QuarkusMain
class Application(
    private val connector: WebSocketConnector<BlueSkyClient>,
    private val logger: Logger,
    private val appScope: ApplicationCoroutineScope
) : QuarkusApplication {

    override fun run(vararg args: String?): Int {
        logger.info("Initializing WebSocket connection...")

        return runCatching {
            val blueSkyUri = URI.create("wss://jetstream.us-east.bsky.network")

            val client = connector.baseUri(blueSkyUri)

            appScope.launch {
                client.connect().awaitSuspending()
            }

            logger.info("WebSocket connected. Awaiting termination signal...")

            Quarkus.waitForExit()

        }.fold({
            0
        }, {
            logger.error("Error running WebSocket daemon", it)
            1
        })

    }
}
