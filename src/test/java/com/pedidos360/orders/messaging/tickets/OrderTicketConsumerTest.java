package com.pedidos360.orders.messaging.tickets;

import com.pedidos360.orders.dto.OrderTicketEventDTO;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.*;

class OrderTicketConsumerTest {

    private final OrderTicketConsumer consumer = new OrderTicketConsumer();
    private final OrderTicketDlqConsumer dlqConsumer = new OrderTicketDlqConsumer();
    private final Channel channel = mock(Channel.class);

    @Test
    @DisplayName("Ticket válido -> ACK explícito")
    void validTicketIsAcked() throws IOException {
        OrderTicketEventDTO dto = new OrderTicketEventDTO(10L, "cli-1", "CREADO", new BigDecimal("25000"), 2, "ok");

        consumer.consumirTicketOrden(dto, channel, 1L, false);

        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    @DisplayName("Ticket con monto inválido -> NACK sin requeue (envío directo a DLQ)")
    void invalidAmountGoesToDlq() throws IOException {
        OrderTicketEventDTO dto = new OrderTicketEventDTO(10L, "cli-1", "CREADO", BigDecimal.ZERO, 1, "x");

        consumer.consumirTicketOrden(dto, channel, 2L, false);

        verify(channel).basicNack(2L, false, false);
    }

    @Test
    @DisplayName("Error transitorio en primer intento -> reintento (requeue=true); en reentrega -> DLQ (requeue=false)")
    void transientRetryThenDlq() throws IOException {
        OrderTicketEventDTO dto = new OrderTicketEventDTO(10L, "cli-1", "CREADO", new BigDecimal("1000"), 1, "RETRY");

        consumer.consumirTicketOrden(dto, channel, 3L, false);
        verify(channel).basicNack(3L, false, true);

        consumer.consumirTicketOrden(dto, channel, 4L, true);
        verify(channel).basicNack(4L, false, false);
    }

    @Test
    @DisplayName("Ticket en la DLQ -> se registra en logs y se confirma (ACK)")
    void dlqTicketIsLoggedAndAcked() throws IOException {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(5L);
        Message message = new Message("{\"orderId\":0}".getBytes(StandardCharsets.UTF_8), props);

        dlqConsumer.procesarTicketEnDlq(message, channel);

        verify(channel).basicAck(5L, false);
    }
}
