package com.pedidos360.orders.messaging.producer;

import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import com.pedidos360.orders.dto.OrderTicketEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Productor desacoplado de eventos asíncronos de órdenes (notificaciones y tickets).
 * Todos los nombres de exchanges y routing keys provienen de {@code application.yml}.
 * Si RabbitMQ no está disponible, registra el error en logs sin interrumpir la transacción principal.
 */
@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.notificaciones.exchange}")
    private String notificacionesExchange;

    @Value("${app.rabbitmq.notificaciones.routing-key}")
    private String notificacionesRoutingKey;

    @Value("${app.rabbitmq.tickets.exchange}")
    private String ticketsExchange;

    @Value("${app.rabbitmq.tickets.routing-key}")
    private String ticketsRoutingKey;

    public OrderEventProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /** Publica una notificación de cambio/creación de orden en el exchange de notificaciones. */
    public void publicarNotificacionOrden(OrderNotificationEventDTO evento) {
        log.info("Publicando notificación asíncrona en RabbitMQ para orden ID: {}", evento.getOrderId());
        try {
            rabbitTemplate.convertAndSend(notificacionesExchange, notificacionesRoutingKey, evento);
        } catch (AmqpException ex) {
            log.error("No se pudo publicar la notificación de la orden ID {} en RabbitMQ (la orden no se ve afectada): {}",
                    evento.getOrderId(), ex.getMessage());
        }
    }

    /** Publica una solicitud asíncrona de generación de ticket/comprobante en el exchange de tickets. */
    public void publicarTicketOrden(OrderTicketEventDTO ticket) {
        log.info("Publicando generación de ticket en RabbitMQ para orden ID: {}", ticket.getOrderId());
        try {
            rabbitTemplate.convertAndSend(ticketsExchange, ticketsRoutingKey, ticket);
        } catch (AmqpException ex) {
            log.error("No se pudo publicar el ticket de la orden ID {} en RabbitMQ (la orden no se ve afectada): {}",
                    ticket.getOrderId(), ex.getMessage());
        }
    }

    /** Alias compatible hacia atrás para publicación de notificación de orden. */
    public void publicarEventoOrden(OrderNotificationEventDTO evento) {
        publicarNotificacionOrden(evento);
    }
}
