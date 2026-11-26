package com.pedidos360.orders.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Consumidor aislado de Apache Kafka para el flujo de streaming de Pedidos360.
 * Imprime en tiempo real Partición, Offset, Key y Mensaje recibido.
 */
@Service
public class OrderKafkaConsumer {

    @KafkaListener(
            topics = "pedidos360-ordenes-streaming",
            groupId = "pedidos360-grupo-streaming"
    )
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        System.out.println("\n" + "=".repeat(90));
        System.out.printf(" [PEDIDOS360 - KAFKA] MENSAJE RECIBIDO EN TIEMPO REAL%n");
        System.out.printf("   Partición : %d%n", partition);
        System.out.printf("   Offset    : %d%n", offset);
        System.out.printf("   Key       : %s%n", key != null ? key : "(sin key)");
        System.out.printf("   Mensaje   : %s%n", message);
        System.out.println("=".repeat(90) + "\n");
    }
}
