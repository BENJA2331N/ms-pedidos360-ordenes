package com.pedidos360.orders.messaging.consumer;

import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrderNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationConsumer.class);

    @RabbitListener(queues = "${app.rabbitmq.ordenes.queue}")
    public void consumirNotificacionOrden(OrderNotificationEventDTO evento,
                                          Channel channel,
                                          @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            log.info("Procesando generación de ticket/notificación para orden: {}", evento);

            // Si el orderId es nulo o menor o igual a 0, forzamos error no recuperable hacia la DLQ
            if (evento.getOrderId() == null || evento.getOrderId() <= 0) {
                throw new IllegalArgumentException("ID de orden inválido para generar ticket: " + evento);
            }

            log.info("Ticket generado y notificación enviada con éxito para orden ID: {}", evento.getOrderId());
            // Confirmación explícita (ACK)
            channel.basicAck(tag, false);

        } catch (IllegalArgumentException ex) {
            log.error("Error de validación en mensaje de orden. Rechazando (NACK) y enviando a DLQ: {}", ex.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception ex) {
            log.error("Fallo inesperado al procesar notificación de orden. Enviando a DLQ: {}", ex.getMessage());
            channel.basicNack(tag, false, false);
        }
    }

    @RabbitListener(queues = "${app.rabbitmq.ordenes.dlq}")
    public void procesarMensajeEnDlq(OrderNotificationEventDTO eventoFallido,
                                     Channel channel,
                                     @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        log.warn("[DLQ ORDENES LOG] Mensaje no entregado capturado en la DLQ: {}", eventoFallido);
        channel.basicAck(tag, false);
    }
}
