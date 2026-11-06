package com.pedidos360.orders.messaging.notificaciones;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Consumidor del dominio funcional "Notificaciones" con ACK manual explícito.
 *
 * <h3>Política documentada de ACK, reintento y envío a DLQ</h3>
 * <ul>
 *   <li><b>Ruta 1 — Éxito ({@code basicAck})</b>: el mensaje es válido, se procesa el despacho
 *       de la notificación y se confirma explícitamente.</li>
 *   <li><b>Ruta 2 — Error no recuperable ({@code basicNack(requeue=false)} -> DLQ)</b>: JSON malformado,
 *       payload nulo, {@code orderId <= 0} o marcador {@code FORCE_DLQ}. Se rechaza sin reencolar para
 *       que RabbitMQ lo envíe directamente a la DLQ mediante {@code x-dead-letter-exchange}.</li>
 *   <li><b>Ruta 3 — Error transitorio ({@code basicNack(requeue=true)} con límite de 1 reintento)</b>:
 *       ante un error recuperable/inesperado en la primera entrega ({@code redelivered == false}),
 *       se reencola para reintento. Si persiste tras la reentrega ({@code redelivered == true}),
 *       se envía a la DLQ con {@code basicNack(requeue=false)}.</li>
 * </ul>
 */
@Component
public class OrderNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationConsumer.class);

    private final ObjectMapper objectMapper;

    public OrderNotificationConsumer() {
        this.objectMapper = new ObjectMapper();
    }

    public OrderNotificationConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "${app.rabbitmq.notificaciones.queue}")
    public void recibirMensajeNotificacion(Message message,
                                           Channel channel,
                                           @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                                           @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered)
            throws IOException {
        if (message == null || message.getBody() == null || message.getBody().length == 0) {
            log.error("[NOTIFICACIONES] Mensaje vacío recibido. Rechazando hacia DLQ (NACK sin requeue).");
            channel.basicNack(tag, false, false);
            return;
        }
        String rawJson = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            OrderNotificationEventDTO evento = objectMapper.readValue(rawJson, OrderNotificationEventDTO.class);
            consumirNotificacionOrden(evento, channel, tag, redelivered);
        } catch (JsonProcessingException ex) {
            log.error("[NOTIFICACIONES] JSON malformado no recuperable ({}). Enviando a DLQ (NACK sin requeue): payload={}",
                    ex.getOriginalMessage(), rawJson);
            channel.basicNack(tag, false, false);
        }
    }

    public void consumirNotificacionOrden(OrderNotificationEventDTO evento,
                                          Channel channel,
                                          long tag,
                                          Boolean redelivered) throws IOException {
        try {
            log.info("[NOTIFICACIONES] Procesando notificación asíncrona para orden: {}", evento);

            if (evento == null || evento.getOrderId() == null || evento.getOrderId() <= 0) {
                throw new IllegalArgumentException("ID de orden inválido para enviar notificación: " + evento);
            }
            if (evento.getMensaje() != null && evento.getMensaje().toUpperCase().contains("FORCE_DLQ")) {
                throw new IllegalArgumentException("Rechazo forzado hacia DLQ en notificación: " + evento.getMensaje());
            }
            if (evento.getMensaje() != null && evento.getMensaje().toUpperCase().contains("RETRY")) {
                throw new IllegalStateException("Fallo temporal simulado en proveedor de notificaciones");
            }

            log.info("[NOTIFICACIONES] Notificación despachada con éxito para orden ID {} (estado={}).",
                    evento.getOrderId(), evento.getStatus());
            channel.basicAck(tag, false);

        } catch (IllegalArgumentException ex) {
            log.error("[NOTIFICACIONES] Error no recuperable en mensaje. Enviando a DLQ (NACK requeue=false): {}",
                    ex.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception ex) {
            if (!Boolean.TRUE.equals(redelivered)) {
                log.warn("[NOTIFICACIONES] Fallo transitorio en primer intento. Reintentando (NACK requeue=true): {}",
                        ex.getMessage());
                channel.basicNack(tag, false, true);
            } else {
                log.error("[NOTIFICACIONES] Reintentos agotados. Enviando a DLQ (NACK requeue=false): {}",
                        ex.getMessage());
                channel.basicNack(tag, false, false);
            }
        }
    }
}
