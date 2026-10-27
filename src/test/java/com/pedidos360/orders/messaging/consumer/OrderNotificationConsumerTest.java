package com.pedidos360.orders.messaging.consumer;

import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.*;

class OrderNotificationConsumerTest {

    private final OrderNotificationConsumer consumer = new OrderNotificationConsumer();
    private final Channel channel = mock(Channel.class);

    @Test
    @DisplayName("Evento válido -> ACK explícito")
    void validEventIsAcked() throws IOException {
        consumer.consumirNotificacionOrden(new OrderNotificationEventDTO(1L, "CREADO", "ok"), channel, 7L, false);

        verify(channel).basicAck(7L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    @DisplayName("Evento inválido (orderId <= 0) -> NACK sin requeue (va a la DLQ)")
    void invalidEventGoesToDlq() throws IOException {
        consumer.consumirNotificacionOrden(new OrderNotificationEventDTO(0L, "CREADO", "x"), channel, 8L, false);

        verify(channel).basicNack(8L, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    @DisplayName("Evento nulo -> NACK sin requeue (va a la DLQ)")
    void nullEventGoesToDlq() throws IOException {
        consumer.consumirNotificacionOrden(null, channel, 9L, null);

        verify(channel).basicNack(9L, false, false);
    }

    @Test
    @DisplayName("Mensaje en la DLQ -> se registra y se confirma (ACK)")
    void dlqMessageIsLoggedAndAcked() throws IOException {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(11L);
        Message message = new Message("{\"orderId\":-1}".getBytes(StandardCharsets.UTF_8), props);

        consumer.procesarMensajeEnDlq(message, channel);

        verify(channel).basicAck(11L, false);
    }
}
