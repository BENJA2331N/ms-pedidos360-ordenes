package com.pedidos360.orders.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Productor REST aislado para publicar eventos de prueba en Apache Kafka.
 * Permite contrastar el envío Sin Key (Round-Robin entre particiones) frente al
 * envío Con Key (hash determinista hacia una misma partición garantizando orden).
 */
@RestController
@RequestMapping("/api/kafka")
public class OrderKafkaController {

    public static final String TOPIC_NAME = "pedidos360-ordenes-streaming";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderKafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/send")
    public String sendMessage(
            @RequestParam String message,
            @RequestParam(required = false) String key) {

        if (key != null && !key.isEmpty()) {
            // Con Key: Hash(key) envía siempre a la misma partición garantizando el orden
            kafkaTemplate.send(TOPIC_NAME, key, message);
            return "Evento enviado al topic '" + TOPIC_NAME + "' CON KEY: " + key + " | MENSAJE: " + message;
        } else {
            // Sin Key: Distribución automática Round-Robin entre particiones 0, 1 y 2
            kafkaTemplate.send(TOPIC_NAME, message);
            return "Evento enviado al topic '" + TOPIC_NAME + "' (SIN KEY - distribución automática) | MENSAJE: " + message;
        }
    }

    @GetMapping("/health")
    public String health() {
        return "Módulo de Streaming Kafka en Pedidos360 operativo";
    }
}
