package com.pedidos360.orders.messaging.tickets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.orders.dto.OrderTicketEventDTO;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Consumidor del dominio funcional "Tickets y Documentos" con ACK manual explícito.
 *
 * <h3>Política documentada de ACK, reintento y envío a DLQ</h3>
 * <ul>
 *   <li><b>Ruta 1 — Éxito ({@code basicAck})</b>: datos del ticket válidos ({@code orderId > 0} y
 *       {@code total > 0}), se emite el comprobante y se confirma explícitamente.</li>
 *   <li><b>Ruta 2 — Error no recuperable ({@code basicNack(requeue=false)} -> DLQ)</b>: JSON malformado,
 *       {@code orderId <= 0}, monto inválido o marcador {@code FORCE_DLQ}. Se envía directo a la DLQ.</li>
 *   <li><b>Ruta 3 — Error transitorio ({@code basicNack(requeue=true)} con 1 reintento)</b>: si ocurre
 *       un fallo recuperable en el primer intento ({@code redelivered == false}), se reencola una vez;
 *       si vuelve a fallar ({@code redelivered == true}), se envía a la DLQ con {@code requeue=false}.</li>
 * </ul>
 */
@Component
public class OrderTicketConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderTicketConsumer.class);

    private final ObjectMapper objectMapper;

    public OrderTicketConsumer() {
        this.objectMapper = new ObjectMapper();
    }

    public OrderTicketConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "${app.rabbitmq.tickets.queue}")
    public void recibirMensajeTicket(Message message,
                                     Channel channel,
                                     @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                                     @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered)
            throws IOException {
        if (message == null || message.getBody() == null || message.getBody().length == 0) {
            log.error("[TICKETS] Mensaje vacío recibido. Rechazando hacia DLQ (NACK sin requeue).");
            channel.basicNack(tag, false, false);
            return;
        }
        String rawJson = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            OrderTicketEventDTO ticket = objectMapper.readValue(rawJson, OrderTicketEventDTO.class);
            consumirTicketOrden(ticket, channel, tag, redelivered);
        } catch (JsonProcessingException ex) {
            log.error("[TICKETS] JSON malformado no recuperable ({}). Enviando a DLQ (NACK sin requeue): payload={}",
                    ex.getOriginalMessage(), rawJson);
            channel.basicNack(tag, false, false);
        }
    }

    public void consumirTicketOrden(OrderTicketEventDTO ticket,
                                    Channel channel,
                                    long tag,
                                    Boolean redelivered) throws IOException {
        try {
            log.info("[TICKETS] Generando comprobante/documento para orden: {}", ticket);

            if (ticket == null || ticket.getOrderId() == null || ticket.getOrderId() <= 0) {
                throw new IllegalArgumentException("ID de orden inválido para emisión de ticket: " + ticket);
            }
            if (ticket.getTotal() == null || ticket.getTotal().compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalArgumentException("Monto total inválido para emisión de ticket: " + ticket.getTotal());
            }
            if (ticket.getDetalle() != null && ticket.getDetalle().toUpperCase().contains("FORCE_DLQ")) {
                throw new IllegalArgumentException("Error forzado en generación de documento: " + ticket.getDetalle());
            }
            if (ticket.getDetalle() != null && ticket.getDetalle().toUpperCase().contains("RETRY")) {
                throw new IllegalStateException("Fallo temporal en motor generador de comprobantes");
            }

            log.info("[TICKETS] Ticket TCK-{} emitido exitosamente para cliente '{}' por total ${}.",
                    ticket.getOrderId(), ticket.getClienteId(), ticket.getTotal());
            channel.basicAck(tag, false);

        } catch (IllegalArgumentException ex) {
            log.error("[TICKETS] Error de validación no recuperable. Enviando a DLQ (NACK requeue=false): {}",
                    ex.getMessage());
            channel.basicNack(tag, false, false);
        } catch (Exception ex) {
            if (!Boolean.TRUE.equals(redelivered)) {
                log.warn("[TICKETS] Fallo transitorio en primer intento. Reintentando (NACK requeue=true): {}",
                        ex.getMessage());
                channel.basicNack(tag, false, true);
            } else {
                log.error("[TICKETS] Reintentos agotados en emisión de ticket. Enviando a DLQ (NACK requeue=false): {}",
                        ex.getMessage());
                channel.basicNack(tag, false, false);
            }
        }
    }
}
