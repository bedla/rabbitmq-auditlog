# RabbitMQ Audit log

RabbitMQ is capable to export audit events based on various actions with the system.
To enable this functionality you have to install plugin `rabbitmq-plugins enable rabbitmq_event_exchange`, see https://www.rabbitmq.com/event-exchange.html for details.

One of the variant how to consume them is by binding to special exchange called `amq.rabbitmq.event` with your custom queue.

This repository demonstrates how to do it with Spring.
Details can be found in [production code](https://github.com/bedla/rabbitmq-auditlog/blob/master/src/main/kotlin/com/example/rabbitmqauditlog/RabbitmqAuditlogApplication.kt) and at [Testcontainers test](https://github.com/bedla/rabbitmq-auditlog/blob/master/src/test/kotlin/com/example/rabbitmqauditlog/RabbitmqAuditlogApplicationTests.kt).
