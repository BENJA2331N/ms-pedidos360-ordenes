package com.pedidos360.orders.messaging.notificaciones;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Consumidor dedicado a la Dead Letter Queue (DLQ) del dominio "Notificaciones".
 * Registra en logs los mensajes fallidos y la causa reportada por RabbitMQ ({@code x-death}).
 */
@Component
public class OrderNotificationDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationDlqConsumer.class);

    @RabbitListener(queues = "${app.rabbitmq.notificaciones.dlq}")
    public void procesarMensajeEnDlq(Message mensaje, Channel channel) throws IOException {
        String cuerpo = new String(mensaje.getBody(), StandardCharsets.UTF_8);
        Object motivo = mensaje.getMessageProperties().getHeader("x-death");
        long tag = mensaje.getMessageProperties().getDeliveryTag();
        log.error("[DLQ NOTIFICACIONES] Mensaje no entregado capturado en DLQ. cuerpo={} x-death={}", cuerpo, motivo);
        channel.basicAck(tag, false);
    }
}
