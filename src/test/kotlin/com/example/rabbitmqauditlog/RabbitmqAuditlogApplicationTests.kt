package com.example.rabbitmqauditlog

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatException
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.core.ConditionTimeoutException
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.awaitility.kotlin.untilTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
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
import org.springframework.boot.web.client.RestTemplateBuilder
import org.springframework.context.annotation.Bean
import org.springframework.core.io.ResourceLoader
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.client.ClientHttpResponse
import org.springframework.web.client.DefaultResponseErrorHandler
import org.springframework.web.client.RestTemplate
import org.springframework.web.client.exchange
import org.springframework.web.util.DefaultUriBuilderFactory
import org.testcontainers.containers.Container
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.output.WaitingConsumer
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import org.zalando.logbook.HeaderFilter
import org.zalando.logbook.Logbook
import org.zalando.logbook.core.DefaultHttpLogFormatter
import org.zalando.logbook.core.DefaultHttpLogWriter
import org.zalando.logbook.core.DefaultSink
import org.zalando.logbook.spring.LogbookClientHttpRequestInterceptor
import java.net.ConnectException
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

    @Autowired
    private lateinit var restTemplateBuilder: RestTemplateBuilder

    private lateinit var restTemplate: RestTemplate

    @BeforeEach
    fun setUp() {
        val logbook = Logbook.builder()
            .sink(
                DefaultSink(
                    DefaultHttpLogFormatter(),
                    DefaultHttpLogWriter()
                )
            )
            .headerFilter(HeaderFilter.none())
            .build();

        restTemplate = restTemplateBuilder
            .errorHandler(object : DefaultResponseErrorHandler() {
                override fun hasError(response: ClientHttpResponse): Boolean {
                    return false
                }
            })
            .interceptors(LogbookClientHttpRequestInterceptor(logbook))
            .uriTemplateHandler(DefaultUriBuilderFactory().also { it.encodingMode = DefaultUriBuilderFactory.EncodingMode.VALUES_ONLY })
            .build()
    }

    @Test
    fun managementUIActionsAreNotLoggedUntilMQTTProtocolIsCalled() {
        val username = "ui-user-test"
        val password = "secret"
        val queueName = "q.ui-user-test"
        val exchangeName1 = "e.ui-user-test-1"
        val exchangeName2 = "e.ui-user-test-2"


        rabbitInvoker.createUser(username, password)
        rabbitInvoker.userSetPermissions(username, ".*", ".*", ".*")
        rabbitInvoker.userSetTags(username, "administrator")
        rabbitInvoker.action { rabbitAdmin, _ ->
            val exchange1 = TopicExchange(exchangeName1)
            val exchange2 = TopicExchange(exchangeName2)
            val queue = Queue(queueName)

            rabbitAdmin.declareExchange(exchange1)
            rabbitAdmin.declareExchange(exchange2)
            rabbitAdmin.declareQueue(queue)
            await atMost 5.seconds.toJavaDuration() until {
                rabbitInvoker.queueExists(queueName)
            }
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange1).with("foo"))
            rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange2).with("foo"))
        }

        restTemplate.exchange<Map<String, Any?>>(
            "http://{host}:{port}/api/whoami",
            HttpMethod.GET,
            HttpEntity<Unit>(HttpHeaders().also { it.setBasicAuth(username, "wrong-pwd") }),
            mapOf("host" to rabbitInvoker.rabbitmqHost(), "port" to rabbitInvoker.rabbitmqUIPort())
        ).also { responseEntity ->
            assertThat(responseEntity.statusCode).isEqualTo(HttpStatus.UNAUTHORIZED)
            assertThat(responseEntity.body)
                .containsEntry("error", "not_authorised")
                .containsEntry("reason", "Login failed")

        }

        restTemplate.exchange<Map<String, Any?>>(
            "http://{host}:{port}/api/whoami",
            HttpMethod.GET,
            HttpEntity<Unit>(HttpHeaders().also { it.setBasicAuth(username, password) }),
            mapOf("host" to rabbitInvoker.rabbitmqHost(), "port" to rabbitInvoker.rabbitmqUIPort())
        ).also { responseEntity ->
            assertThat(responseEntity.statusCode).isEqualTo(HttpStatus.OK)
            assertThat(responseEntity.body)
                .containsEntry("name", username)
                .containsEntry("tags", "administrator")
        }

        restTemplate.exchange<Map<String, Any?>>(
            "http://{host}:{port}/api/queues/{vhost}/{queue-name}?lengths_age=60&lengths_incr=5&msg_rates_age=60&msg_rates_incr=5&data_rates_age=60&data_rates_incr=5",
            HttpMethod.GET,
            HttpEntity<Unit>(HttpHeaders().also { it.setBasicAuth(username, password) }),
            mapOf("host" to rabbitInvoker.rabbitmqHost(), "port" to rabbitInvoker.rabbitmqUIPort(), "queue-name" to queueName, "vhost" to "/")
        ).also { responseEntity ->
            assertThat(responseEntity.statusCode).isEqualTo(HttpStatus.OK)
            assertThat(responseEntity.body)
                .containsEntry("name", queueName)
        }

        assertThatThrownBy {
            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "user.authentication.success" &&
                            it.messageProperties.headers["name"] == username
                }.also { message ->
                    message.logAsJson("authenticateUserSuccess")
                } != null
            }
        }.isInstanceOf(ConditionTimeoutException::class.java)

        rabbitInvoker.action { rabbitAdmin, _ ->
            rabbitAdmin.rabbitTemplate.send(exchangeName1, "foo", Message("Hello world".toByteArray()))
            rabbitAdmin.rabbitTemplate.send(exchangeName2, "foo", Message("Wolf on his way".toByteArray()))
        }

        // give RabbitMQ some time to process incoming message
        Thread.sleep(3.seconds.inWholeMilliseconds)

        restTemplate.exchange<List<Any?>>(
            "http://{host}:{port}/api/queues/{vhost}/{queue-name}/get",
            HttpMethod.POST,
            HttpEntity<Map<String, Any>>(
                mapOf("vhost" to "/", "name" to queueName, "truncate" to "50000", "ackmode" to "ack_requeue_true", "encoding" to "auto", "count" to "999"),
                HttpHeaders().also { it.setBasicAuth(username, password); it.contentType = MediaType.APPLICATION_JSON; it.accept = listOf(MediaType.APPLICATION_JSON) }
            ),
            mapOf("host" to rabbitInvoker.rabbitmqHost(), "port" to rabbitInvoker.rabbitmqUIPort(), "queue-name" to queueName, "vhost" to "/")
        ).also { responseEntity ->
            assertThat(responseEntity.statusCode).isEqualTo(HttpStatus.OK)
            assertThat(responseEntity.body)
                .hasSize(2)
                .isInstanceOf(List::class.java)
            assertThat(responseEntity.body!![0] as Map<String, Any>)
                .containsEntry("payload", "Hello world")
            assertThat(responseEntity.body!![1] as Map<String, Any>)
                .containsEntry("payload", "Wolf on his way")
        }

        // delete exchange using RabbitMQClient
        rabbitInvoker.action(username, password) { rabbitAdmin, _ ->
            rabbitAdmin.deleteExchange(exchangeName1)
        }

        // delete exchange using ManagementUI call
        restTemplate.exchange<Map<String, Any?>>(
            "http://{host}:{port}/api/exchanges/{vhost}/{exchange-name}",
            HttpMethod.DELETE,
            HttpEntity<Unit>(HttpHeaders().also { it.setBasicAuth(username, password) }),
            mapOf("host" to rabbitInvoker.rabbitmqHost(), "port" to rabbitInvoker.rabbitmqUIPort(), "exchange-name" to exchangeName2, "vhost" to "/")
        ).also { responseEntity ->
            assertThat(responseEntity.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
            assertThat(responseEntity.body).isNull()
        }

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "exchange.deleted" &&
                        it.messageProperties.headers["name"] == exchangeName1 &&
                        it.messageProperties.headers["user_who_performed_action"] == username
            }.also { message ->
                message.logAsJson("deleteExchange 1")
            } != null
        }

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "exchange.deleted" &&
                        it.messageProperties.headers["name"] == exchangeName2 &&
                        it.messageProperties.headers["user_who_performed_action"] == username
            }.also { message ->
                message.logAsJson("deleteExchange 2")
            } != null
        }

        // TODO For some reason, this returns 406 Not Acceptable, BUT when same is called with curl, it return valid 204 No Content
        //      Find it and finish this test
//        restTemplate.exchange<Map<String, Any?>>(
//            "http://{host}:{port}/api/queues/{vhost}/{queue-name}/contents",
//            HttpMethod.DELETE,
//            HttpEntity<Map<String, Any>>(
//                mapOf("vhost" to "/", "name" to queueName, "mode" to "purge"),
//                HttpHeaders().also { it.setBasicAuth(username, password)/*; it.contentType = MediaType.APPLICATION_JSON; it.accept = listOf(MediaType.APPLICATION_JSON)*/ }
//            ),
//            mapOf("host" to rabbitInvoker.rabbitmqHost(), "port" to rabbitInvoker.rabbitmqUIPort(), "queue-name" to queueName, "vhost" to "/")
//        ).also { responseEntity ->
//            assertThat(responseEntity.statusCode).isEqualTo(HttpStatus.NO_CONTENT)
//            assertThat(responseEntity.body).isNull()
//        }
    }

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
            val queueName = "q.declare-queue-test"

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
            val queueName = "q.delete-queue-test"

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
            val queueName = "q.declare-binding-queue-test"

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
            val queueName = "q.declare-binding-queue-test"

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

        rabbitInvoker.createVHost(vHostName)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.firstOrNull {
                it.messageProperties.receivedRoutingKey == "vhost.created" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == vHostName
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

        rabbitInvoker.createVHost(vHostName)

        await atMost 5.seconds.toJavaDuration() until {
            receiver.auditMessages.any {
                it.messageProperties.receivedRoutingKey == "vhost.created" &&
                        it.messageProperties.headers["user_who_performed_action"] == "rmq-cli" &&
                        it.messageProperties.headers["name"] == vHostName
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
        try {
            await atMost 5.seconds.toJavaDuration() until {
                receiver.auditMessages.firstOrNull {
                    it.messageProperties.receivedRoutingKey == "permission.deleted" &&
                            it.messageProperties.headers["user"] == username &&
                            it.messageProperties.headers["user_who_performed_action"] == "rmq-cli"
                }.also { message ->
                    message.logAsJson("clearUserPermissionsTopic")
                } != null
            }
        } catch (e: ConditionTimeoutException) {
            logger.error("Unable to find particular conditional audit-message, what I got is: {}", receiver.auditMessages)
            throw e
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
        val queueName = "q.authenticate-user-success-test"

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
        val queueName = "q.consumer-queue-test"

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
        val queueName = "q.consumer-entity-forbidden-queue-test"

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
                .hasMessageContaining("reply-text=ACCESS_REFUSED - access to queue '$queueName' in vhost '/' refused for user '$username',")
//                .hasMessageContaining("reply-text=ACCESS_REFUSED - read access to queue '$queueName' in vhost '/' refused for user '$username',")
        }


        await atMost 5.seconds.toJavaDuration() until {
            logConsumer.frames.any {
                it.utf8String.contains("operation basic.consume caused a channel exception access_refused: ") &&
//                        it.utf8String.contains("read access to queue '$queueName' in vhost '/' refused for user '$username'")
                        it.utf8String.contains("access to queue '$queueName' in vhost '/' refused for user '$username'")
            }
        }
    }

    @Test
    fun authenticateUserFailure() {
        val username = "authenticate-user-failure-test"
        val password = "secret"
        val queueName = "q.authenticate-user-failure-test"

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
//        }.withRootCauseInstanceOf(AuthenticationFailureException::class.java)
        }.withRootCauseInstanceOf(ConnectException::class.java)
//            .withMessageContaining("ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN.")
            .withMessageContaining("java.net.ConnectException: Connection timed out: getsockopt")

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

    @Disabled
    @Test
    fun connectionChannel() {
        val username = "my-connection-channel"

        rabbitInvoker.createUser(username, "secret")

        await atMost 5.seconds.toJavaDuration() until {
            println(receiver.auditMessages)
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

    /*

    [
    (Body:'[B@29150811(byte[0])'
    MessageProperties [
        headers={
            destination_kind=queue,
            timestamp_in_ms=1758714314067,
            vhost=/,
            source_kind=exchange,
            arguments=[],
            destination_name=my-audit-queue,
            routing_key=binding.*,
            user_who_performed_action=guest,
            source_name=amq.rabbitmq.event
        },
        timestamp=Wed Sep 24 13:45:14 CEST 2025,
        contentLength=0,
        receivedDeliveryMode=PERSISTENT,
        redelivered=false,
        receivedExchange=amq.rabbitmq.event,
        receivedRoutingKey=binding.created,
        deliveryTag=1,
        consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA,
        consumerQueue=my-audit-queue
    ]
    ),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314208, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=consumer.*, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=2, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314338, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=vhost.*, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=3, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314451, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=user.#, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=4, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314586, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=permission.*, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=5, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314702, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=topic.permission.*, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=6, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314826, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=connection.*, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=7, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={destination_kind=queue, timestamp_in_ms=1758714314956, vhost=/, source_kind=exchange, arguments=[], destination_name=my-audit-queue, routing_key=channel.*, user_who_performed_action=guest, source_name=amq.rabbitmq.event}, timestamp=Wed Sep 24 13:45:14 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=binding.created, deliveryTag=8, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={timestamp_in_ms=1758714315062, name=my-tech-user, user_who_performed_action=guest}, timestamp=Wed Sep 24 13:45:15 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=user.created, deliveryTag=9, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={timestamp_in_ms=1758714315063, name=my-tech-user, user_who_performed_action=guest, tags=[administrator]}, timestamp=Wed Sep 24 13:45:15 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=user.tags.set, deliveryTag=10, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={timestamp_in_ms=1758714315191, vhost=/, read=.*, configure=.*, user=my-tech-user, user_who_performed_action=guest, write=.*}, timestamp=Wed Sep 24 13:45:15 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=permission.created, deliveryTag=11, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={connection_name=172.17.0.1:46070 -> 172.17.0.3:5672, timestamp_in_ms=1758714315722, protocol={0,9,1}, connection_type=network, peer_host={0,0,0,0,0,65535,44049,1}, host={0,0,0,0,0,65535,44049,3}, name=guest, peer_port=46070, auth_mechanism=PLAIN, ssl=false}, timestamp=Wed Sep 24 13:45:15 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=user.authentication.success, deliveryTag=12, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])'
    MessageProperties [
        headers={
            ssl_cipher=,
            pid=<rabbit@abefe01aed0d.1758714310.911.0>,
            connected_at=1758714315679,
            type=network,
            ssl=false,
            timeout=60,
            frame_max=131072,
            protocol={0,9,1},
            client_properties=[
                {<<"connection_name">>,longstr,<<"rabbitConnectionFactory#5a466dd:0">>},
                {<<"product">>,longstr,<<"RabbitMQ">>},
                {<<"copyright">>,longstr,<<"Copyright (c) 2007-2024 Broadcom Inc. and/or its subsidiaries.">>},
                {<<"capabilities">>,table,[
                    {<<"exchange_exchange_bindings">>,bool,true},
                    {<<"connection.blocked">>,bool,true},
                    {<<"authentication_failure_close">>,bool,true},
                    {<<"basic.nack">>,bool,true},
                    {<<"publisher_confirms">>,bool,true},
                    {<<"consumer_cancel_notify">>,bool,true}
                    ]
                },
                {<<"information">>,longstr,<<"Licensed under the MPL. See https://www.rabbitmq.com/">>},
                {<<"version">>,longstr,<<"5.25.0">>},
                {<<"platform">>,longstr,<<"Java">>}
            ],
            host={0,0,0,0,0,65535,44049,3},
            auth_mechanism=PLAIN,
            user_who_performed_action=guest,
            ssl_protocol=,
            user_provided_name=rabbitConnectionFactory#5a466dd:0,
            peer_cert_subject=,
            ssl_key_exchange=,
            peer_cert_validity=,
            peer_port=46070,
            ssl_hash=,
            peer_cert_issuer=,
            node=rabbit@abefe01aed0d,
            timestamp_in_ms=1758714315728,
            vhost=/,
            channel_max=2047,
            peer_host={0,0,0,0,0,65535,44049,1},
            port=5672,
            name=172.17.0.1:46070 -> 172.17.0.3:5672,
            user=guest},
        timestamp=Wed Sep 24 13:45:15 CEST 2025,
        contentLength=0,
        receivedDeliveryMode=PERSISTENT,
        redelivered=false,
        receivedExchange=amq.rabbitmq.event,
        receivedRoutingKey=connection.created,
        deliveryTag=13,
        consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA,
        consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])'
    MessageProperties [
        headers={
            number=1,
            timestamp_in_ms=1758714315744,
            vhost=/,
            name=172.17.0.1:46070 -> 172.17.0.3:5672 (1),
            connection=<rabbit@abefe01aed0d.1758714310.911.0>,
            pid=<rabbit@abefe01aed0d.1758714310.921.0>,
            user=guest,
            user_who_performed_action=guest
        },
        timestamp=Wed Sep 24 13:45:15 CEST 2025,
        contentLength=0,
        receivedDeliveryMode=PERSISTENT,
        redelivered=false,
        receivedExchange=amq.rabbitmq.event,
        receivedRoutingKey=channel.created,
        deliveryTag=14,
        consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA,
        consumerQueue=my-audit-queue
    ]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={timestamp_in_ms=1758714315775, vhost=/, prefetch_count=250, consumer_tag=amq.ctag-55sa55A79KkpP5H3RgsOlA, channel=<rabbit@abefe01aed0d.1758714310.921.0>, exclusive=false, arguments=[], ack_required=true, user_who_performed_action=guest, queue=my-audit-queue}, timestamp=Wed Sep 24 13:45:15 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=consumer.created, deliveryTag=15, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue]),
    (Body:'[B@29150811(byte[0])' MessageProperties [headers={timestamp_in_ms=1758714317199, name=my-connection-channel, user_who_performed_action=rmq-cli}, timestamp=Wed Sep 24 13:45:17 CEST 2025, contentLength=0, receivedDeliveryMode=PERSISTENT, redelivered=false, receivedExchange=amq.rabbitmq.event, receivedRoutingKey=user.created, deliveryTag=16, consumerTag=amq.ctag-55sa55A79KkpP5H3RgsOlA, consumerQueue=my-audit-queue])]

     */

//    connection.created
//    connection.closed
//    channel.created
//    channel.closed

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
        return RabbitMQContainer(DockerImageName.parse("rabbitmq:3.7.25-management-alpine"))
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
    fun rabbitmqHost() = rabbit.host!!

    fun rabbitmqPort() = rabbit.amqpPort!!

    fun rabbitmqUIPort() = rabbit.httpPort!!

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
            "rabbitmqadmin", "--format", "raw_json", "-V", "/", "list", "bindings"
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

    fun createVHost(vHostName: String) {
        execInContainer(
            "Unable to add_vhost",
            "rabbitmqctl", "add_vhost", vHostName
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
