package com.pedidos360.orders.messaging.producer;

import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
        rabbitTemplate.convertAndSend(exchange, routingKey, evento);
    }
}
