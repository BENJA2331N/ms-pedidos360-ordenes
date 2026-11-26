package com.pedidos360.orders.kafka;

import com.pedidos360.orders.kafka.domain.OrderStreamingDomainService;
import com.pedidos360.orders.kafka.dto.InventoryStreamingEventDTO;
import com.pedidos360.orders.kafka.dto.OrderStreamingEventDTO;
import com.pedidos360.orders.kafka.producer.OrderKafkaProducer;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Controlador REST del módulo de streaming Kafka en ms-pedidos360-ordenes.
 * Permite publicar eventos DTO tipados en ambos tópicos, probar particionamiento
 * con/sin key y consultar métricas procesadas por los Consumer Groups.
 */
@RestController
@RequestMapping("/api/kafka")
public class OrderKafkaController {

    public static final String TOPIC_ORDENES = "pedidos360.ordenes.eventos";
    public static final String TOPIC_INVENTARIO = "pedidos360.inventario.eventos";

    private final OrderKafkaProducer orderKafkaProducer;
    private final OrderStreamingDomainService domainService;

    public OrderKafkaController(OrderKafkaProducer orderKafkaProducer,
                                OrderStreamingDomainService domainService) {
        this.orderKafkaProducer = orderKafkaProducer;
        this.domainService = domainService;
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> publishOrderEvent(
            @Valid @RequestBody OrderStreamingEventDTO dto,
            @RequestParam(defaultValue = "true") boolean useKey) {
        orderKafkaProducer.publicarEventoOrden(dto, useKey);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "mensaje", "Evento DTO de orden enviado a Kafka",
                "topico", TOPIC_ORDENES,
                "conKey", useKey,
                "evento", dto
        ));
    }

    @PostMapping("/inventory")
    public ResponseEntity<Map<String, Object>> publishInventoryEvent(
            @Valid @RequestBody InventoryStreamingEventDTO dto) {
        orderKafkaProducer.publicarEventoInventario(dto);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "mensaje", "Evento DTO de inventario enviado a Kafka",
                "topico", TOPIC_INVENTARIO,
                "evento", dto
        ));
    }

    /**
     * Endpoint rápido para demostraciones de particionamiento Con Key (Sticky) vs Sin Key (Round-Robin)
     * construyendo un OrderStreamingEventDTO estructurado.
     */
    @GetMapping("/send")
    public String sendMessage(
            @RequestParam String message,
            @RequestParam(required = false) String key) {

        OrderStreamingEventDTO dto = new OrderStreamingEventDTO(
                100L,
                "cliente-demo",
                "STREAMING",
                new BigDecimal("19990"),
                1,
                message
        );
        orderKafkaProducer.publicarEventoOrdenConKeyPersonalizada(key, dto);

        if (key != null && !key.isBlank()) {
            return "Evento DTO enviado al topic '" + TOPIC_ORDENES + "' CON KEY: " + key + " | MENSAJE: " + message;
        }
        return "Evento DTO enviado al topic '" + TOPIC_ORDENES + "' (SIN KEY - distribución automática) | MENSAJE: " + message;
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> metrics() {
        return ResponseEntity.ok(domainService.obtenerResumenMetricas());
    }

    @GetMapping("/health")
    public String health() {
        return "Módulo de Streaming Kafka en Pedidos360 operativo";
    }
}
