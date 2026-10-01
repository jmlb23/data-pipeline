package com.github.jmlb23.data.kafka

import io.quarkus.websockets.next.OnClose
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.delay
import org.jboss.logging.Logger
import java.net.URI
import io.quarkus.websockets.next.WebSocketClient
import io.quarkus.websockets.next.OnOpen
import io.quarkus.websockets.next.OnTextMessage
import io.quarkus.websockets.next.OnError
import io.smallrye.mutiny.Uni
import io.smallrye.mutiny.onFailure
import io.smallrye.reactive.messaging.MutinyEmitter
import kotlinx.coroutines.launch
import org.eclipse.microprofile.reactive.messaging.Channel

@ApplicationScoped
@WebSocketClient(path = "/xrpc/network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post")
class BlueSkyClient(
    private val logger: Logger,
    @Channel("bluesky-jetstream")
    private val kafkaEmitter: MutinyEmitter<String>,
) {

    @OnOpen
    fun onOpen() {
        logger.info("OPENED")
    }

    @OnTextMessage
    fun onMessage(message: String): Uni<String> {
        logger.info(message)
        return kafkaEmitter
            .send(message)
            .map { "ok" }
            .onFailure()
            .recoverWithItem("ko")
    }

    @OnClose
    fun onClose() {
        kafkaEmitter.complete()
    }

    @OnError
    fun onError(throwable: Throwable) {
        throwable.printStackTrace()
    }
}
