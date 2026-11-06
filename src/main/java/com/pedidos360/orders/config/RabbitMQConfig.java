package com.pedidos360.orders.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración centralizada de RabbitMQ para el microservicio de órdenes.
 * Declara un conjunto dedicado de beans {@link Exchange}, {@link Queue} y {@link Binding}
 * (más su respectiva DLX y DLQ) para <b>cada caso de uso</b>, leyendo todos los nombres
 * desde {@code application.yml} sin mezclar lógica de negocio.
 *
 * <pre>
 * Caso de uso 1 — Notificaciones de estado de orden:
 *   OrderEventProducer --(notificaciones.routing-key)--> [notificaciones.exchange (direct)]
 *                                                               |
 *                                                               v
 *                                                    [notificaciones.queue] --> OrderNotificationConsumer (ACK manual)
 *                                                               |  (NACK requeue=false)
 *                                                               v
 *                                                    [notificaciones.dlx] ----> [notificaciones.dlq] --> OrderNotificationDlqConsumer
 *
 * Caso de uso 2 — Generación asíncrona de tickets / comprobantes:
 *   OrderEventProducer --(tickets.routing-key)---------> [tickets.exchange (direct)]
 *                                                               |
 *                                                               v
 *                                                    [tickets.queue] ---------> OrderTicketConsumer (ACK manual)
 *                                                               |  (NACK requeue=false)
 *                                                               v
 *                                                    [tickets.dlx] -----------> [tickets.dlq] ---------> OrderTicketDlqConsumer
 * </pre>
 */
@Configuration
public class RabbitMQConfig {

    // ==================== Caso de uso 1: Notificaciones ====================
    @Value("${app.rabbitmq.notificaciones.exchange}")
    private String notificacionesExchangeName;

    @Value("${app.rabbitmq.notificaciones.queue}")
    private String notificacionesQueueName;

    @Value("${app.rabbitmq.notificaciones.routing-key}")
    private String notificacionesRoutingKey;

    @Value("${app.rabbitmq.notificaciones.dlx}")
    private String notificacionesDlxName;

    @Value("${app.rabbitmq.notificaciones.dlq}")
    private String notificacionesDlqName;

    @Value("${app.rabbitmq.notificaciones.dlq-routing-key}")
    private String notificacionesDlqRoutingKey;

    // ==================== Caso de uso 2: Tickets ====================
    @Value("${app.rabbitmq.tickets.exchange}")
    private String ticketsExchangeName;

    @Value("${app.rabbitmq.tickets.queue}")
    private String ticketsQueueName;

    @Value("${app.rabbitmq.tickets.routing-key}")
    private String ticketsRoutingKey;

    @Value("${app.rabbitmq.tickets.dlx}")
    private String ticketsDlxName;

    @Value("${app.rabbitmq.tickets.dlq}")
    private String ticketsDlqName;

    @Value("${app.rabbitmq.tickets.dlq-routing-key}")
    private String ticketsDlqRoutingKey;

    // -------------------- Beans Caso de Uso 1: Notificaciones --------------------

    @Bean
    public DirectExchange notificacionesDeadLetterExchange() {
        return new DirectExchange(notificacionesDlxName);
    }

    @Bean
    public Queue notificacionesDeadLetterQueue() {
        return QueueBuilder.durable(notificacionesDlqName).build();
    }

    @Bean
    public Binding notificacionesDlqBinding() {
        return BindingBuilder.bind(notificacionesDeadLetterQueue())
                .to(notificacionesDeadLetterExchange())
                .with(notificacionesDlqRoutingKey);
    }

    @Bean
    public DirectExchange notificacionesExchange() {
        return new DirectExchange(notificacionesExchangeName);
    }

    @Bean
    public Queue notificacionesQueue() {
        return QueueBuilder.durable(notificacionesQueueName)
                .deadLetterExchange(notificacionesDlxName)
                .deadLetterRoutingKey(notificacionesDlqRoutingKey)
                .build();
    }

    @Bean
    public Binding notificacionesBinding() {
        return BindingBuilder.bind(notificacionesQueue())
                .to(notificacionesExchange())
                .with(notificacionesRoutingKey);
    }

    // -------------------- Beans Caso de Uso 2: Tickets --------------------

    @Bean
    public DirectExchange ticketsDeadLetterExchange() {
        return new DirectExchange(ticketsDlxName);
    }

    @Bean
    public Queue ticketsDeadLetterQueue() {
        return QueueBuilder.durable(ticketsDlqName).build();
    }

    @Bean
    public Binding ticketsDlqBinding() {
        return BindingBuilder.bind(ticketsDeadLetterQueue())
                .to(ticketsDeadLetterExchange())
                .with(ticketsDlqRoutingKey);
    }

    @Bean
    public DirectExchange ticketsExchange() {
        return new DirectExchange(ticketsExchangeName);
    }

    @Bean
    public Queue ticketsQueue() {
        return QueueBuilder.durable(ticketsQueueName)
                .deadLetterExchange(ticketsDlxName)
                .deadLetterRoutingKey(ticketsDlqRoutingKey)
                .build();
    }

    @Bean
    public Binding ticketsBinding() {
        return BindingBuilder.bind(ticketsQueue())
                .to(ticketsExchange())
                .with(ticketsRoutingKey);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
