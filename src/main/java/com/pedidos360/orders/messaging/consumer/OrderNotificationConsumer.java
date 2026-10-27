package com.pedidos360.orders.messaging.consumer;

import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Consumidores del dominio "órdenes" (ACK manual configurado en application.yml).
 * <p>
 * Política de confirmación y errores de la cola principal:
 * <ul>
 *   <li><b>Éxito</b> -> {@code basicAck}.</li>
 *   <li><b>Error no recuperable</b> (mensaje inválido, {@link IllegalArgumentException}) ->
 *       {@code basicNack(requeue=false)}: RabbitMQ lo desvía a la DLQ vía x-dead-letter-exchange.</li>
 *   <li><b>Error inesperado</b> (posiblemente transitorio) -> un único <b>reintento</b>
 *       ({@code basicNack(requeue=true)}); si el mensaje ya venía reentregado -> {@code basicNack(requeue=false)} a la DLQ.</li>
 * </ul>
 * Los mensajes que llegan a la DLQ se registran en logs y se confirman.
 */
@Component
public class OrderNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationConsumer.class);

    @RabbitListener(queues = "${app.rabbitmq.ordenes.queue}")
    public void consumirNotificacionOrden(OrderNotificationEventDTO evento,
                                          Channel channel,
                                          @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                                          @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered)
            throws IOException {
        try {
            log.info("Procesando generación de ticket/notificación para orden: {}", evento);

            // Si el orderId es nulo o menor o igual a 0, forzamos error no recuperable hacia la DLQ
            if (evento == null || evento.getOrderId() == null || evento.getOrderId() <= 0) {
                throw new IllegalArgumentException("ID de orden inválido para generar ticket: " + evento);
            }

            log.info("Ticket generado y notificación enviada con éxito para orden ID: {}", evento.getOrderId());
            // Confirmación explícita (ACK)
            channel.basicAck(tag, false);

        } catch (IllegalArgumentException ex) {
            log.error("Error de validación en mensaje de orden. Rechazando (NACK) y enviando a DLQ: {}", ex.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception ex) {
            if (!Boolean.TRUE.equals(redelivered)) {
                log.warn("Fallo inesperado al procesar notificación de orden. Reintentando una vez (NACK con requeue): {}",
                        ex.getMessage());
                channel.basicNack(tag, false, true);
            } else {
                log.error("Fallo persistente tras reintento. Enviando a DLQ (NACK sin requeue): {}", ex.getMessage());
                channel.basicNack(tag, false, false);
            }
        }
    }

    /**
     * Registra en logs los mensajes no entregados. Recibe el mensaje crudo (sin convertir) para poder
     * registrar también mensajes con JSON inválido.
     */
    @RabbitListener(queues = "${app.rabbitmq.ordenes.dlq}")
    public void procesarMensajeEnDlq(Message mensaje, Channel channel) throws IOException {
        String cuerpo = new String(mensaje.getBody(), StandardCharsets.UTF_8);
        Object motivo = mensaje.getMessageProperties().getHeader("x-death");
        log.warn("[DLQ ORDENES LOG] Mensaje no entregado capturado en la DLQ. cuerpo={} x-death={}", cuerpo, motivo);
        channel.basicAck(mensaje.getMessageProperties().getDeliveryTag(), false);
    }
}
