package com.pedidos360.orders.kafka.producer;

import com.pedidos360.orders.kafka.dto.InventoryStreamingEventDTO;
import com.pedidos360.orders.kafka.dto.OrderStreamingEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Productor tipado de Apache Kafka para ms-pedidos360-ordenes.
 * Publica DTOs estructurados hacia los 2 tópicos del ecosistema de forma asíncrona y resiliente
 * (si Kafka no está disponible, se registra en logs sin interrumpir BD ni RabbitMQ).
 */
@Component
public class OrderKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderKafkaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordenesTopic;
    private final String inventarioTopic;

    public OrderKafkaProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${app.kafka.topics.ordenes-eventos.name:pedidos360.ordenes.eventos}") String ordenesTopic,
            @Value("${app.kafka.topics.inventario-eventos.name:pedidos360.inventario.eventos}") String inventarioTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordenesTopic = ordenesTopic;
        this.inventarioTopic = inventarioTopic;
    }

    /**
     * Publica un evento de orden usando orderId como key para garantizar orden estricto por pedido,
     * o sin key cuando useOrderKey es false (distribución Round-Robin entre particiones).
     */
    public void publicarEventoOrden(OrderStreamingEventDTO dto, boolean useOrderKey) {
        String key = (useOrderKey && dto.getOrderId() != null) ? "orden-" + dto.getOrderId() : null;
        enviar(ordenesTopic, key, dto);
    }

    public void publicarEventoOrdenConKeyPersonalizada(String customKey, OrderStreamingEventDTO dto) {
        String key = (customKey != null && !customKey.isBlank()) ? customKey.trim() : null;
        enviar(ordenesTopic, key, dto);
    }

    /**
     * Publica un evento en el segundo tópico (pedidos360.inventario.eventos) con key por producto.
     */
    public void publicarEventoInventario(InventoryStreamingEventDTO dto) {
        String key = dto.getProductId() != null ? "producto-" + dto.getProductId() : null;
        enviar(inventarioTopic, key, dto);
    }

    private void enviar(String topic, String key, Object payload) {
        try {
            var future = (key != null)
                    ? kafkaTemplate.send(topic, key, payload)
                    : kafkaTemplate.send(topic, payload);

            if (future != null) {
                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[KAFKA-PRODUCER] Error asíncrono al publicar en topic={} key={}: {}",
                                topic, key, ex.getMessage());
                    } else if (result != null && result.getRecordMetadata() != null) {
                        log.info("[KAFKA-PRODUCER] Evento publicado | topic={} partition={} offset={} key={} payload={}",
                                topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                key != null ? key : "(sin key)",
                                payload);
                    }
                });
            }
        } catch (Exception ex) {
            log.error("[KAFKA-PRODUCER] No se pudo publicar en Kafka topic={} (el flujo principal y RabbitMQ continúan intactos): {}",
                    topic, ex.getMessage());
        }
    }
}
