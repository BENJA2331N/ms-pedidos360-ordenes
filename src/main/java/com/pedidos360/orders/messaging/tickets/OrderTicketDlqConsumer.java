package com.pedidos360.orders.messaging.tickets;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Consumidor dedicado a la Dead Letter Queue (DLQ) del dominio "Tickets y Documentos".
 * Registra en logs los comprobantes fallidos y su motivo ({@code x-death}), confirmando el mensaje.
 */
@Component
public class OrderTicketDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderTicketDlqConsumer.class);

    @RabbitListener(queues = "${app.rabbitmq.tickets.dlq}")
    public void procesarTicketEnDlq(Message mensaje, Channel channel) throws IOException {
        String cuerpo = new String(mensaje.getBody(), StandardCharsets.UTF_8);
        Object motivo = mensaje.getMessageProperties().getHeader("x-death");
        long tag = mensaje.getMessageProperties().getDeliveryTag();
        log.error("[DLQ TICKETS] Documento/ticket fallido registrado en DLQ. cuerpo={} x-death={}", cuerpo, motivo);
        channel.basicAck(tag, false);
    }
}
