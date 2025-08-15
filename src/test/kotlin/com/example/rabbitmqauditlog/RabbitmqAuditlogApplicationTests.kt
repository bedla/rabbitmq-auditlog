package com.example.rabbitmqauditlog

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AuthenticationFailureException
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.amqp.CachingConnectionFactoryConfigurer
import org.springframework.boot.autoconfigure.amqp.RabbitConnectionFactoryBeanConfigurer
import org.springframework.boot.autoconfigure.amqp.RabbitProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.core.io.ResourceLoader
import org.testcontainers.containers.Container
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.output.WaitingConsumer
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration


const val techUserName = "my-tech-user"
const val techUserPassword = "my-tech-user-password"
const val rootVHost = "/"
const val auditExchangeName = "amq.rabbitmq.event"
const val auditQueueName = "my-audit-queue"


@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(
    classes = [
        TestConfig::class
    ]
)
class RabbitmqAuditlogApplicationTests {
    @Autowired
    private lateinit var rabbitInvoker: RabbitInvoker

    @Autowired
    private lateinit var receiver: AuditLogReceiver

    @Autowired
    private lateinit var logConsumer: WaitingConsumer

    @Test
    fun declareExchange() {
        rabbitInvoker.action { rabbitAdmin, _ ->
            val exchangeName = "declare-exchange-test"

            val exchange = TopicExchange(exchangeName)
            rabbitAdmin.declareExchange(exchange)

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "exchange.created" &&
                            it.messageProperties.headers["name"] == exchangeName &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }.also { message ->
                    message.logAsJson("declareExchange")
                } != null
            }

            assertThat(rabbitInvoker.exchangeExists(exchangeName))
                .isTrue()
        }
    }

    @Test
    fun deleteExchange() {
        rabbitInvoker.action { rabbitAdmin, _ ->
            val exchangeName = "delete-exchange-test"

            val exchange = TopicExchange(exchangeName)
            rabbitAdmin.declareExchange(exchange)

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.any {
                    it.messageProperties.receivedRoutingKey == "exchange.created" &&
                            it.messageProperties.headers["name"] == exchangeName &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }
            }
            assertThat(rabbitInvoker.exchangeExists(exchangeName))
                .isTrue()

            rabbitAdmin.deleteExchange(exchangeName)

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "exchange.deleted" &&
                            it.messageProperties.headers["name"] == exchangeName &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }.also { message ->
                    message.logAsJson("deleteExchange")
                } != null
            }

            assertThat(rabbitInvoker.exchangeExists(exchangeName))
                .isFalse()
        }
    }

    @Test
    fun declareQueue() {
        rabbitInvoker.action { rabbitAdmin, _ ->
            val queueName = "declare-queue-test"

            rabbitAdmin.declareQueue(Queue(queueName))

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "queue.created" &&
                            it.messageProperties.headers["name"] == queueName &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }.also { message ->
                    message.logAsJson("declareQueue")
                } != null
            }

            assertThat(rabbitInvoker.queueExists(queueName))
                .isTrue()
        }
    }

    @Test
    fun deleteQueue() {
        rabbitInvoker.action { rabbitAdmin, _ ->
            val queueName = "delete-queue-test"

            rabbitAdmin.declareQueue(Queue(queueName))

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.any {
                    it.messageProperties.receivedRoutingKey == "queue.created" &&
                            it.messageProperties.headers["name"] == queueName &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }
            }
            assertThat(rabbitInvoker.queueExists(queueName))
                .isTrue()

            rabbitAdmin.deleteQueue(queueName)

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "queue.deleted" &&
                            it.messageProperties.headers["name"] == queueName &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }.also { message ->
                    message.logAsJson("deleteQueue")
                } != null
            }

            assertThat(rabbitInvoker.queueExists(queueName))
                .isFalse()
        }
    }

    @Test
    fun declareBinding() {
        rabbitInvoker.action { rabbitAdmin, _ ->

            val exchangeName = "declare-binding-exchange-test"
            val queueName = "declare-binding-queue-test"

            val exchange = TopicExchange(exchangeName)
            val queue = Queue(queueName)
            rabbitAdmin.declareExchange(exchange)
            rabbitAdmin.declareQueue(queue)
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("foo"))

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "binding.created" &&
                            it.messageProperties.headers["source_kind"] == "exchange" &&
                            it.messageProperties.headers["destination_kind"] == "queue" &&
                            it.messageProperties.headers["source_name"] == exchangeName &&
                            it.messageProperties.headers["destination_name"] == queueName &&
                            it.messageProperties.headers["routing_key"] == "foo" &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }.also { message ->
                    message.logAsJson("declareBinding")
                } != null
            }

            assertThat(rabbitInvoker.bindingExists(exchangeName, queueName, "foo"))
                .isTrue()
        }
    }

    @Test
    fun deleteBinding() {
        rabbitInvoker.action { rabbitAdmin, _ ->

            val exchangeName = "declare-binding-exchange-test"
            val queueName = "declare-binding-queue-test"

            val exchange = TopicExchange(exchangeName)
            val queue = Queue(queueName)
            val binding = BindingBuilder.bind(queue).to(exchange).with("foo")

            rabbitAdmin.declareExchange(exchange)
            rabbitAdmin.declareQueue(queue)
            rabbitAdmin.declareBinding(binding)

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.any {
                    it.messageProperties.receivedRoutingKey == "binding.created" &&
                            it.messageProperties.headers["source_kind"] == "exchange" &&
                            it.messageProperties.headers["destination_kind"] == "queue" &&
                            it.messageProperties.headers["source_name"] == exchangeName &&
                            it.messageProperties.headers["destination_name"] == queueName &&
                            it.messageProperties.headers["routing_key"] == "foo" &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }
            }

            assertThat(rabbitInvoker.bindingExists(exchangeName, queueName, "foo"))
                .isTrue()

            rabbitAdmin.removeBinding(binding)

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "binding.deleted" &&
                            it.messageProperties.headers["source_kind"] == "exchange" &&
                            it.messageProperties.headers["destination_kind"] == "queue" &&
                            it.messageProperties.headers["source_name"] == exchangeName &&
                            it.messageProperties.headers["destination_name"] == queueName &&
                            it.messageProperties.headers["routing_key"] == "foo" &&
                            it.messageProperties.headers["user_who_performed_action"] == techUserName
                }.also { message ->
                    message.logAsJson("deleteBinding")
                } != null
            }

            assertThat(rabbitInvoker.bindingExists(exchangeName, queueName, "foo"))
                .isFalse()
        }
    }

    @Test
    fun createVHost() {
        val vHostName = "create-vhost-test"

        rabbitInvoker.createVHost(vHostName, "my description", "foo", "bar")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "vhost.created" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == vHostName &&
                        it.messageProperties.headers["description"] == "my description" &&
                        it.messageProperties.headers["tags"] == listOf("foo", "bar")
            }.also { message ->
                message.logAsJson("createVHost")
            } != null
        }

        assertThat(rabbitInvoker.vhostExists(vHostName))
            .isTrue()
    }

    @Test
    fun deleteVHost() {
        val vHostName = "delete-vhost-test"

        rabbitInvoker.createVHost(vHostName, "my description", "foo", "bar")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.any {
                it.messageProperties.receivedRoutingKey == "vhost.created" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == vHostName &&
                        it.messageProperties.headers["description"] == "my description" &&
                        it.messageProperties.headers["tags"] == listOf("foo", "bar")
            }
        }

        assertThat(rabbitInvoker.vhostExists(vHostName))
            .isTrue()

        rabbitInvoker.deleteVHost(vHostName)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "vhost.deleted" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == vHostName
            }.also { message ->
                message.logAsJson("deleteVHost")
            } != null
        }

        assertThat(rabbitInvoker.vhostExists(vHostName))
            .isFalse()
    }

    @Test
    fun createUser() {
        val username = "create-user-test"

        rabbitInvoker.createUser(username, "secret")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.created" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == username
            }.also { message ->
                message.logAsJson("createUser")
            } != null
        }

        assertThat(rabbitInvoker.userExists(username))
            .isTrue()
    }

    @Test
    fun deleteUser() {
        val username = "delete-user-test"

        rabbitInvoker.createUser(username, "secret")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.any {
                it.messageProperties.receivedRoutingKey == "user.created" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == username
            }
        }

        assertThat(rabbitInvoker.userExists(username))
            .isTrue()

        rabbitInvoker.deleteUser(username)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.deleted" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == username
            }.also { message ->
                message.logAsJson("deleteUser")
            } != null
        }

        assertThat(rabbitInvoker.userExists(username))
            .isFalse()
    }

    @Test
    fun setUserPermissions() {
        val username = "set-user-permissions-test"
        val password = "secret"

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, ".foo", "bar", "xxx")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "permission.created" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["read"] == "xxx" &&
                        it.messageProperties.headers["write"] == "bar" &&
                        it.messageProperties.headers["configure"] == ".foo"
            }.also { message ->
                message.logAsJson("setUserPermissions")
            } != null
        }
    }

    @Test
    fun userChangePassword() {
        val username = "user-change-password-test"
        val password = "secret"

        rabbitInvoker.createUser(username, password)

        await atMost 5.seconds.toJavaDuration() until {
            rabbitInvoker.userExists(username)
        }

        rabbitInvoker.userChangePassword(username, "i-am-new-password")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.password.changed" &&
                        it.messageProperties.headers["name"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli"
            }.also { message ->
                message.logAsJson("userChangePassword")
            } != null
        }
    }

    @Test
    fun userClearPassword() {
        val username = "user-clear-password-test"
        val password = "secret"

        rabbitInvoker.createUser(username, password)

        await atMost 5.seconds.toJavaDuration() until {
            rabbitInvoker.userExists(username)
        }

        rabbitInvoker.userClearPassword(username)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.password.cleared" &&
                        it.messageProperties.headers["name"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli"
            }.also { message ->
                message.logAsJson("userClearPassword")
            } != null
        }
    }

    @Test
    fun setUserTags() {
        val username = "set-user-tags-test"
        val password = "secret"

        rabbitInvoker.createUser(username, password)

        await atMost 5.seconds.toJavaDuration() until {
            rabbitInvoker.userExists(username)
        }

        rabbitInvoker.userSetTags(username, "foo", "bar")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.tags.set" &&
                        it.messageProperties.headers["name"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["tags"] == listOf("foo", "bar")
            }.also { message ->
                message.logAsJson("setUserTags SET")
            } != null
        }

        rabbitInvoker.userSetTags(username)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.tags.set" &&
                        it.messageProperties.headers["name"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["tags"] == emptyList<Any>()
            }.also { message ->
                message.logAsJson("setUserTags CLEAR")
            } != null
        }
    }

    @Test
    fun clearUserPermissions() {
        val username = "clear-user-permissions-test"
        val password = "secret"

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, ".foo", "bar", "xxx")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.any {
                it.messageProperties.receivedRoutingKey == "permission.created" &&
                        it.messageProperties.headers["user"] == username
            }
        }

        rabbitInvoker.userClearPermissions(username)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "permission.deleted" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli"
            }.also { message ->
                message.logAsJson("clearUserPermissions")
            } != null
        }
    }

    @Test
    fun setUserPermissionsTopic() {
        val username = "set-user-permissions-topic-test"
        val password = "secret"

        val exchangeName = "set-user-permissions-topic-exchange-test"

        rabbitInvoker.action { rabbitAdmin, _ ->
            val exchange = TopicExchange(exchangeName)
            rabbitAdmin.declareExchange(exchange)
        }

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissionsTopic(username, exchangeName, "xxx", "yyy")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "topic.permission.created" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["read"] == "yyy" &&
                        it.messageProperties.headers["write"] == "xxx" &&
                        it.messageProperties.headers["exchange"] == exchangeName
            }.also { message ->
                message.logAsJson("setUserPermissionsTopic")
            } != null
        }
    }

    @Test
    fun clearUserPermissionsTopic() {
        val username = "clear-user-permissions-topic-test"
        val password = "secret"

        val exchangeName = "clear-user-permissions-topic-exchange-test"

        rabbitInvoker.action { rabbitAdmin, _ ->
            val exchange = TopicExchange(exchangeName)
            rabbitAdmin.declareExchange(exchange)
        }

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissionsTopic(username, exchangeName, "xxx", "yyy")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.any {
                it.messageProperties.receivedRoutingKey == "topic.permission.created" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["exchange"] == exchangeName
            }
        }

        rabbitInvoker.userClearPermissionsTopic(username, exchangeName)

        // This should be topic.permission.deleted not permission.deleted
        // see https://github.com/rabbitmq/rabbitmq-server/blob/v3.12.8/deps/rabbitmq_event_exchange/test/system_SUITE.erl#L398-L417
        // fix detected in v4.1.2
        // bug reported here https://github.com/rabbitmq/rabbitmq-server/issues/9937
        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "topic.permission.deleted" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli"
            }.also { message ->
                message.logAsJson("clearUserPermissionsTopic")
            } != null
        }
    }

    @Test
    fun clearUserPermissionsTopicNoExchangeNameSpecified() {
        val username = "clear-user-permissions-topic-test-2"
        val password = "secret"

        val exchangeName = "clear-user-permissions-topic-exchange-test-2"

        rabbitInvoker.action { rabbitAdmin, _ ->
            val exchange = TopicExchange(exchangeName)
            rabbitAdmin.declareExchange(exchange)
        }

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissionsTopic(username, exchangeName, "xxx", "yyy")

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "topic.permission.created" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["exchange"] == exchangeName
            }.also { message ->
                message.logAsJson("clearUserPermissionsTopicNoExchangeNameSpecified (exchange not empty)")
            } != null
        }

        rabbitInvoker.userClearPermissionsTopic(username)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "topic.permission.deleted" &&
                        it.messageProperties.headers["user"] == username &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["user"] == "clear-user-permissions-topic-test-2"
            }.also { message ->
                message.logAsJson("clearUserPermissionsTopicNoExchangeNameSpecified (empty exchange)")
            } != null
        }
    }

    @Test
    fun authenticateUserSuccess() {
        val username = "authenticate-user-success-test"
        val password = "secret"
        val queueName = "authenticate-user-success-test"

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, ".*", ".*", ".*")
        rabbitInvoker.action { rabbitAdmin, _ ->
            rabbitAdmin.declareQueue(Queue(queueName))
            await atMost 5.seconds.toJavaDuration() until {
                rabbitInvoker.queueExists(queueName)
            }
        }

        rabbitInvoker.action(username, password) { rabbitAdmin, _ ->
            // TCP round trip
            rabbitAdmin.getQueueInfo(queueName)
        }

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.authentication.success" &&
                        it.messageProperties.headers["name"] == username
            }.also { message ->
                message.logAsJson("authenticateUserSuccess")
            } != null
        }
    }

    @Test
    fun consumerConnectedAndDisconnected() {
        val username = "consumer-test"
        val password = "secret"
        val exchangeName = "consumer-exchange-test"
        val queueName = "consumer-queue-test"

        val exchange = TopicExchange(exchangeName)
        val queue = Queue(queueName)

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, ".*", ".*", ".*")
        rabbitInvoker.action { rabbitAdmin, _ ->
            rabbitAdmin.declareExchange(exchange)
            rabbitAdmin.declareQueue(queue)
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("foo"))
        }

        val consumed = AtomicBoolean()
        rabbitInvoker.action(username, password) { rabbitAdmin, _ ->
            val consumerTag = rabbitAdmin.rabbitTemplate.execute { channel ->
                val consumer = object : DefaultConsumer(channel) {
                    override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: ByteArray) {
                        consumed.set(true)
                    }
                }
                channel.basicConsume(queueName, true, consumer)
            }!!

            rabbitAdmin.rabbitTemplate.send(exchangeName, "foo", Message("Hello world".toByteArray()))

            await atMost 5.seconds.toJavaDuration() untilTrue consumed

            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.any {
                    it.messageProperties.receivedRoutingKey == "consumer.created" &&
                            it.messageProperties.headers["queue"] == queueName &&
                            it.messageProperties.headers["user_who_performed_action"] == username
                }
            }

            rabbitAdmin.rabbitTemplate.execute { channel ->
                channel.basicCancel(consumerTag)
            }
        }

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.any {
                it.messageProperties.receivedRoutingKey == "consumer.deleted" &&
                        it.messageProperties.headers["queue"] == queueName &&
                        it.messageProperties.headers["user_who_performed_action"] == username
            }
        }
    }

    @Test
    fun consumerAccessToForbiddenEntity() {
        val username = "consumer-entity-forbidden-test"
        val password = "secret"
        val exchangeName = "consumer-entity-forbidden-exchange-test"
        val queueName = "consumer-entity-forbidden-queue-test"

        val exchange = TopicExchange(exchangeName)
        val queue = Queue(queueName)

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, "", "aaaWrite", "bbbRead")
        rabbitInvoker.action { rabbitAdmin, _ ->
            rabbitAdmin.declareExchange(exchange)
            rabbitAdmin.declareQueue(queue)
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with("foo"))
        }

        val consumed = AtomicBoolean()
        rabbitInvoker.action(username, password) { rabbitAdmin, _ ->
            assertThatThrownBy {
                rabbitAdmin.rabbitTemplate.execute { channel ->
                    val consumer = object : DefaultConsumer(channel) {
                        override fun handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: ByteArray) {
                            consumed.set(true)
                        }
                    }
                    channel.basicConsume(queueName, true, consumer)
                }
            }.rootCause()
                .isInstanceOf(ShutdownSignalException::class.java)
                .hasMessageContaining("#method<channel.close>(reply-code=403,")
                .hasMessageContaining("reply-text=ACCESS_REFUSED - read access to queue '$queueName' in vhost '/' refused for user '$username',")
        }


        await atMost 5.seconds.toJavaDuration() until {
            logConsumer.frames.any {
                it.utf8String.contains("operation basic.consume caused a channel exception access_refused: ") &&
                        it.utf8String.contains("read access to queue '$queueName' in vhost '/' refused for user '$username'")
            }
        }
    }

    @Test
    fun authenticateUserFailure() {
        val username = "authenticate-user-failure-test"
        val password = "secret"
        val queueName = "authenticate-user-failure-test"

        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, ".*", ".*", ".*")
        rabbitInvoker.action { rabbitAdmin, _ ->
            rabbitAdmin.declareQueue(Queue(queueName))
            await atMost 5.seconds.toJavaDuration() until {
                rabbitInvoker.queueExists(queueName)
            }
        }

        assertThatException().isThrownBy {
            rabbitInvoker.action(username, "wrong-password") { rabbitAdmin, _ ->
                // TCP round trip
                rabbitAdmin.getQueueInfo(queueName)
            }
        }.withRootCauseInstanceOf(AuthenticationFailureException::class.java)
            .withMessageContaining("ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN.")

        await atMost 5.seconds.toJavaDuration() until {
            val errorWithMessage = { properties: MessageProperties, expectedError: String ->
                val list = (properties.headers["error"] as? List<*>)
                    ?.filterIsInstance<Number>()
                    ?.map { it.toByte() }
                if (list == null) {
                    false
                } else {
                    list.toByteArray().decodeToString() == expectedError
                }
            }

            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "user.authentication.failure" &&
                        it.messageProperties.headers["name"] == username &&
                        errorWithMessage(it.messageProperties, "user '$username' - invalid credentials")
            }.also { message ->
                message.logAsJson("authenticateUserFailure")
            } != null
        }
    }


//    policy.set
//    policy.cleared
//    queue.policy.updated
//    queue.policy.cleared
//    parameter.set
//    parameter.cleared

//    vhost.limits.set
//    vhost.limits.cleared

//    alarm.set
//    alarm.cleared

//    shovel.worker.status
//    shovel.worker.removed

//    federation.link.status
//    federation.link.removed

    companion object {
        private val logger = LoggerFactory.getLogger(RabbitmqAuditlogApplicationTests::class.java)!!
        private val objectMapper = ObjectMapper()

        private fun Message?.logAsJson(prefixMessage: String) {
            if (this != null) {
                logger.info(prefixMessage + ": " + objectMapper.writeValueAsString(this))
            }
        }
    }
}

@TestConfiguration
class TestConfig {
    @Bean
    fun logConsumer(): WaitingConsumer = WaitingConsumer()

    @Bean
    @ServiceConnection
    fun rabbitMQContainer(logConsumer: WaitingConsumer): RabbitMQContainer {
        return RabbitMQContainer(DockerImageName.parse("rabbitmq:management"))
            .withPluginsEnabled("rabbitmq_event_exchange")
            .withQueue(rootVHost, auditQueueName)
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "queue.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "exchange.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "binding.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "consumer.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "vhost.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "user.#", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "permission.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "topic.permission.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "connection.*", "queue")
            .withBinding(rootVHost, auditExchangeName, auditQueueName, emptyMap(), "channel.*", "queue")
            .withUser(techUserName, techUserPassword, setOf("administrator"))
            .withPermission(rootVHost, techUserName, ".*", ".*", ".*")
            .withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("RabbitMQ Container")))
            .withLogConsumer(logConsumer)
    }

    @Bean
    fun rabbitInvoker(
        rabbit: RabbitMQContainer,
        resourceLoader: ResourceLoader,
        objectMapper: ObjectMapper
    ): RabbitInvoker {
        return RabbitInvoker(rabbit, resourceLoader, objectMapper)
    }
}

class RabbitInvoker(
    private val rabbit: RabbitMQContainer,
    private val resourceLoader: ResourceLoader,
    private val objectMapper: ObjectMapper
) {
    fun action(
        rabbitMQUsername: String,
        rabbitMQPassword: String,
        code: (RabbitAdmin, RabbitMQContainer) -> Unit
    ) {
        var userConnectionFactory: CachingConnectionFactory? = null
        try {
            userConnectionFactory = userConnectionFactory(
                rabbitMQUsername,
                rabbitMQPassword,
                rabbit.host,
                rabbit.amqpPort,
                resourceLoader
            )

            val rabbitAdmin = RabbitAdmin(userConnectionFactory)
            code(rabbitAdmin, rabbit)
        } finally {
            userConnectionFactory?.destroy()
        }
    }

    fun action(code: (RabbitAdmin, RabbitMQContainer) -> Unit) {
        action(techUserName, techUserPassword, code)
    }

    fun exchangeExists(exchangeName: String): Boolean {
        return execInContainer(
            "Unable to get exchanges",
            "rabbitmqadmin", "-V", "/", "list", "exchanges"
        ) { execResult ->
            execResult.stdout.contains(exchangeName)
        }
    }

    fun queueExists(queueName: String): Boolean {
        return execInContainer(
            "Unable to get queues",
            "rabbitmqadmin", "-V", "/", "list", "queues"
        ) { execResult ->
            execResult.stdout.contains(queueName)
        }
    }

    fun bindingExists(source: String, destination: String, routingKey: String): Boolean {
        return execInContainer(
            "Unable to get queues",
            "rabbitmqadmin", "--format", "pretty_json", "-V", "/", "list", "bindings"
        ) { execResult ->

            data class BindingRow(
                val arguments: Map<String, Any?>?,
                val destination: String,
                @JsonProperty("destination_type")
                val destinationType: String,
                @JsonProperty("properties_key")
                val propertiesKey: String,
                @JsonProperty("routing_key")
                val routingKey: String,
                val source: String,
                val vhost: String
            )

            val list = objectMapper.readValue(execResult.stdout, Array<BindingRow>::class.java)!!
            list.asSequence()
                .filter { it.source == source }
                .filter { it.destination == destination }
                .filter { it.routingKey == routingKey }
                .any()
        }
    }

    fun vhostExists(vhostName: String): Boolean {
        return execInContainer(
            "Unable to list vhosts",
            "rabbitmqadmin", "-V", "/", "list", "vhosts"
        ) { execResult ->
            execResult.stdout.contains(vhostName)
        }
    }

    fun userExists(username: String): Boolean {
        return execInContainer(
            "Unable to list users",
            "rabbitmqadmin", "-V", "/", "list", "users"
        ) { execResult ->
            execResult.stdout.contains(username)
        }
    }

    fun createVHost(
        vHostName: String,
        description: String,
        vararg tags: String
    ) {
        execInContainer(
            "Unable to add_vhost",
            "rabbitmqctl", "add_vhost", vHostName, "--description", description, "--tags", tags.joinToString(",")
        )
    }

    fun deleteVHost(
        vHostName: String
    ) {
        execInContainer(
            "Unable to delete_vhost",
            "rabbitmqctl", "delete_vhost", vHostName
        )
    }

    fun createUser(
        username: String,
        password: String
    ) {
        execInContainer(
            "Unable to add_user",
            "rabbitmqctl", "add_user", username, password
        )
    }

    fun userChangePassword(
        username: String,
        password: String
    ) {
        execInContainer(
            "Unable to change_password",
            "rabbitmqctl", "change_password", username, password
        )
    }

    fun userClearPassword(username: String) {
        execInContainer(
            "Unable to clear_password",
            "rabbitmqctl", "clear_password", username
        )
    }

    fun userSetTags(username: String, vararg tags: String) {
        execInContainer(
            "Unable to set_user_tags",
            "rabbitmqctl", "set_user_tags", username, *tags
        )
    }

    fun deleteUser(username: String) {
        execInContainer(
            "Unable to delete_user",
            "rabbitmqctl", "delete_user", username
        )
    }

    fun userSetPermissions(username: String, permissionConfigure: String, permissionWrite: String, permissionRead: String) {
        execInContainer(
            "Unable to set_permissions for user",
            "rabbitmqctl", "set_permissions", "-p", "/", username, permissionConfigure, permissionWrite, permissionRead
        )
    }

    fun userClearPermissions(username: String) {
        execInContainer(
            "Unable to clear_permissions user",
            "rabbitmqctl", "clear_permissions", "-p", "/", username
        )
    }

    fun userSetPermissionsTopic(username: String, topicExchangeName: String, permissionWrite: String, permissionRead: String) {
        execInContainer(
            "Unable to set_topic_permissions user",
            "rabbitmqctl", "set_topic_permissions", "-p", "/", username, topicExchangeName, permissionWrite, permissionRead
        )
    }

    fun userClearPermissionsTopic(username: String, topicExchangeName: String) {
        execInContainer(
            "Unable to clear_topic_permissions for user",
            "rabbitmqctl", "clear_topic_permissions", "-p", "/", username, topicExchangeName
        )
    }

    fun userClearPermissionsTopic(username: String) {
        execInContainer(
            "Unable to clear_topic_permissions for user",
            "rabbitmqctl", "clear_topic_permissions", "-p", "/", username
        )
    }

    private fun execInContainer(errorMessage: String, vararg command: String, code: (Container.ExecResult) -> Boolean): Boolean {
        val execResult = rabbit.execInContainer(*command)
        return if (execResult.exitCode == 0) {
            code(execResult)
        } else {
            logger.error("$errorMessage\n::: stdout :::\n${execResult.stdout}\n---\n::: stderr :::\n${execResult.stderr}")
            throw IllegalStateException(errorMessage)
        }
    }

    private fun execInContainer(errorMessage: String, vararg command: String) {
        execInContainer(errorMessage, *command) { _ -> true }
    }

    private fun userConnectionFactory(
        username: String,
        password: String,
        host: String,
        port: Int,
        resourceLoader: ResourceLoader
    ): CachingConnectionFactory {
        val rabbitProperties = RabbitProperties().also {
            it.username = username
            it.password = password
            it.host = host
            it.port = port
        }

        val connectionFactoryBean = RabbitConnectionFactoryBean()
        RabbitConnectionFactoryBeanConfigurer(resourceLoader, rabbitProperties).also {
            it.configure(connectionFactoryBean)
        }

        val connectionFactory = with(connectionFactoryBean) {
            afterPropertiesSet()
            getObject()
        }

        val factory = CachingConnectionFactory(connectionFactory)

        CachingConnectionFactoryConfigurer(rabbitProperties).also {
            it.configure(factory)
        }

        return factory
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RabbitInvoker::class.java)!!
    }
}
