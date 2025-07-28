package com.example.rabbitmqauditlog

import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.CopyOnWriteArrayList


@SpringBootApplication
class RabbitmqAuditlogApplication

fun main(args: Array<String>) {
    runApplication<RabbitmqAuditlogApplication>(*args)
}


@Configuration
class Config {
    @Bean
    fun queue(): Queue {
        return Queue(auditQueueName, true)
    }

    @Bean
    fun container(
            connectionFactory: ConnectionFactory,
            listenerAdapter: MessageListenerAdapter
    ): SimpleMessageListenerContainer {
        val container = SimpleMessageListenerContainer()
        container.connectionFactory = connectionFactory
        container.setQueueNames(auditQueueName)
        container.setMessageListener(listenerAdapter)
        return container
    }

    @Bean
    fun listenerAdapter(receiver: AuditLogReceiver): MessageListenerAdapter {
        return object : MessageListenerAdapter(receiver, "receiveMessage") {
            override fun buildListenerArguments(extractedMessage: Any?, channel: Channel?, message: Message?): Array<Any> {
                return arrayOf(extractedMessage!!, message!!)
            }
        }
    }

    companion object {
        const val auditQueueName = "my-audit-queue"
    }
}


@Component
class AuditLogReceiver {
    private var _messages = CopyOnWriteArrayList<Message>()
    val auditMessages: List<Message>
        get() {
            return _messages
        }

    fun receiveMessage(body: ByteArray, message: Message) {
        logger.info("### ${message.dumpMessageToStr()}")
        _messages.add(message)
    }


    companion object {
        private val logger = LoggerFactory.getLogger(AuditLogReceiver::class.java)!!
    }
}


fun Message.dumpMessageToStr(): String {
    val action = messageProperties.receivedRoutingKey
    val who = messageProperties.headers["user_who_performed_action"]
    val sourceKind = messageProperties.headers["source_kind"]
    val destinationKind = messageProperties.headers["destination_kind"]
    val routingKey = messageProperties.headers["routing_key"]
    val obj = messageProperties.headers["timestamp_in_ms"]
    val timestamp = (obj as? Number)?.toLong()?.toZonedDateTime()
    return "$action, who=$who, $sourceKind -> $destinationKind ::: $routingKey - $timestamp ... $this"
}

private fun Long.toZonedDateTime(): ZonedDateTime {
    val instant = Instant.ofEpochMilli(this)
    return ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
}
