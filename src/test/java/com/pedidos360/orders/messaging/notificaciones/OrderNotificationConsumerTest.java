package com.pedidos360.orders.messaging.notificaciones;

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
    private final OrderNotificationDlqConsumer dlqConsumer = new OrderNotificationDlqConsumer();
    private final Channel channel = mock(Channel.class);

    @Test
    @DisplayName("Evento válido -> ACK explícito")
    void validEventIsAcked() throws IOException {
        consumer.consumirNotificacionOrden(new OrderNotificationEventDTO(1L, "CREADO", "ok"), channel, 7L, false);

        verify(channel).basicAck(7L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    @DisplayName("Evento inválido (orderId <= 0) -> NACK sin requeue (envío directo a DLQ)")
    void invalidEventGoesToDlq() throws IOException {
        consumer.consumirNotificacionOrden(new OrderNotificationEventDTO(0L, "CREADO", "x"), channel, 8L, false);

        verify(channel).basicNack(8L, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    @DisplayName("Error transitorio en primer intento -> NACK con requeue=true (reintento)")
    void transientErrorIsRequeuedOnce() throws IOException {
        consumer.consumirNotificacionOrden(new OrderNotificationEventDTO(2L, "CREADO", "RETRY"), channel, 9L, false);

        verify(channel).basicNack(9L, false, true);
    }

    @Test
    @DisplayName("Error transitorio tras reentrega -> NACK con requeue=false (va a la DLQ)")
    void redeliveredTransientErrorGoesToDlq() throws IOException {
        consumer.consumirNotificacionOrden(new OrderNotificationEventDTO(2L, "CREADO", "RETRY"), channel, 10L, true);

        verify(channel).basicNack(10L, false, false);
    }

    @Test
    @DisplayName("JSON malformado -> NACK sin requeue hacia DLQ")
    void malformedJsonGoesToDlq() throws IOException {
        Message message = new Message("{bad-json".getBytes(StandardCharsets.UTF_8), new MessageProperties());

        consumer.recibirMensajeNotificacion(message, channel, 11L, false);

        verify(channel).basicNack(11L, false, false);
    }

    @Test
    @DisplayName("Mensaje en la DLQ -> se registra y se confirma (ACK)")
    void dlqMessageIsLoggedAndAcked() throws IOException {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(12L);
        Message message = new Message("{\"orderId\":-1}".getBytes(StandardCharsets.UTF_8), props);

        dlqConsumer.procesarMensajeEnDlq(message, channel);

        verify(channel).basicAck(12L, false);
    }
}
