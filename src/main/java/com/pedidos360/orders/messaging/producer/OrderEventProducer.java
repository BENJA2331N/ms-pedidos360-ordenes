package com.pedidos360.orders.messaging.producer;

import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Productor de eventos de órdenes. Los nombres de exchange y routing key vienen de application.yml.
 * <p>
 * La mensajería es una tarea secundaria y asíncrona: si RabbitMQ no está disponible, el fallo se
 * registra en logs pero NO se propaga, para no afectar (ni revertir) la lógica de negocio de órdenes.
 */
@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.ordenes.exchange}")
    private String exchange;

    @Value("${app.rabbitmq.ordenes.routing-key}")
    private String routingKey;

    public OrderEventProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publicarEventoOrden(OrderNotificationEventDTO evento) {
        log.info("Enviando evento asíncrono a RabbitMQ para orden ID: {}", evento.getOrderId());
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, evento);
        } catch (AmqpException ex) {
            log.error("No se pudo publicar el evento de la orden ID {} en RabbitMQ (la orden no se ve afectada): {}",
                    evento.getOrderId(), ex.getMessage());
        }
    }
}
