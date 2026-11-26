package com.pedidos360.orders.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.orders.kafka.consumer.OrderKafkaConsumers;
import com.pedidos360.orders.kafka.domain.OrderStreamingDomainService;
import com.pedidos360.orders.kafka.dto.InventoryStreamingEventDTO;
import com.pedidos360.orders.kafka.dto.OrderStreamingEventDTO;
import com.pedidos360.orders.kafka.producer.OrderKafkaProducer;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderKafkaController.class)
@Import(OrderStreamingDomainService.class)
class OrderKafkaTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderStreamingDomainService domainService;

    @MockBean
    private OrderKafkaProducer orderKafkaProducer;

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    @DisplayName("POST /api/kafka/orders valida DTO y delega al productor de Tópico 1")
    void postValidOrderEvent() throws Exception {
        OrderStreamingEventDTO dto = new OrderStreamingEventDTO(
                10L, "cliente-abc", "CREADO", new BigDecimal("45990"), 2, "test");

        mockMvc.perform(post("/api/kafka/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.topico").value(OrderKafkaController.TOPIC_ORDENES));

        verify(orderKafkaProducer).publicarEventoOrden(any(OrderStreamingEventDTO.class), eq(true));
    }

    @Test
    @DisplayName("POST /api/kafka/orders rechaza DTO inválido con 400 Bad Request")
    void postInvalidOrderEventReturns400() throws Exception {
        OrderStreamingEventDTO invalid = new OrderStreamingEventDTO();

        mockMvc.perform(post("/api/kafka/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalid)))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(orderKafkaProducer);
    }

    @Test
    @DisplayName("POST /api/kafka/inventory valida DTO y delega al productor de Tópico 2")
    void postValidInventoryEvent() throws Exception {
        InventoryStreamingEventDTO dto = new InventoryStreamingEventDTO(
                5L, "Teclado Mecánico", "DESCUENTO_STOCK", 2, 8, "test");

        mockMvc.perform(post("/api/kafka/inventory")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.topico").value(OrderKafkaController.TOPIC_INVENTARIO));

        verify(orderKafkaProducer).publicarEventoInventario(any(InventoryStreamingEventDTO.class));
    }

    @Test
    @DisplayName("GET /api/kafka/send soporta envío Con Key y Sin Key")
    void sendEndpointSupportsKeyAndWithoutKey() throws Exception {
        mockMvc.perform(get("/api/kafka/send").param("message", "PedidoNuevo-ClienteA"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("SIN KEY")));

        mockMvc.perform(get("/api/kafka/send")
                        .param("message", "Paso1-OrdenCreada")
                        .param("key", "orden-555"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("CON KEY: orden-555")));
    }

    @Test
    @DisplayName("Consumidores validan payloads, delegan al servicio de dominio y rechazan eventos corruptos")
    void consumersValidateAndDelegateToDomain() {
        OrderKafkaConsumers consumers = new OrderKafkaConsumers(domainService, validator);

        OrderStreamingEventDTO validOrder = new OrderStreamingEventDTO(
                77L, "cli-77", "CREADO", new BigDecimal("10000"), 1, "unit-test");
        consumers.consumirOrdenParaAnalitica(validOrder, OrderKafkaController.TOPIC_ORDENES, 0, 1L, "orden-77");
        consumers.consumirOrdenParaAuditoria(validOrder, OrderKafkaController.TOPIC_ORDENES, 0, 1L, "orden-77");

        InventoryStreamingEventDTO validInv = new InventoryStreamingEventDTO(
                3L, "Mouse Pro", "DESCUENTO", 1, 14, "ms-catalogo");
        consumers.consumirEventoInventario(validInv, OrderKafkaController.TOPIC_INVENTARIO, 1, 0L, "producto-3");

        Map<String, Object> metricas = domainService.obtenerResumenMetricas();
        assertTrue(((Number) metricas.get("totalEventosAnalitica")).longValue() >= 1L);

        // Evento inválido -> lanza IllegalArgumentException para ir directo al DLT sin reintentar
        OrderStreamingEventDTO corrupt = new OrderStreamingEventDTO();
        assertThrows(IllegalArgumentException.class, () ->
                consumers.consumirOrdenParaAnalitica(corrupt, OrderKafkaController.TOPIC_ORDENES, 0, 2L, null));

        // Fallo transitorio simulado -> lanza RuntimeException para activar backoff de reintentos
        OrderStreamingEventDTO transientFail = new OrderStreamingEventDTO(
                78L, "cli-78", "CREADO", new BigDecimal("5000"), 1, "SIMULAR_FALLO_TRANSITORIO");
        assertThrows(RuntimeException.class, () ->
                consumers.consumirOrdenParaAnalitica(transientFail, OrderKafkaController.TOPIC_ORDENES, 0, 3L, "orden-78"));
    }

    @Test
    @DisplayName("OrderKafkaProducer envía a ambos tópicos usando KafkaTemplate de manera resiliente")
    void producerPublishesToBothTopicsAndIsolatesBrokerErrors() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        when(template.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));

        OrderKafkaProducer producer = new OrderKafkaProducer(
                template, OrderKafkaController.TOPIC_ORDENES, OrderKafkaController.TOPIC_INVENTARIO);

        OrderStreamingEventDTO orderDto = new OrderStreamingEventDTO(
                15L, "cli-1", "ACEPTADO", new BigDecimal("25000"), 2, "test");
        producer.publicarEventoOrden(orderDto, true);
        verify(template).send(eq(OrderKafkaController.TOPIC_ORDENES), eq("orden-15"), eq(orderDto));

        InventoryStreamingEventDTO invDto = new InventoryStreamingEventDTO(
                9L, "Monitor", "ALTA", 10, 10, "test");
        producer.publicarEventoInventario(invDto);
        verify(template).send(eq(OrderKafkaController.TOPIC_INVENTARIO), eq("producto-9"), eq(invDto));

        // Si Kafka lanza excepción síncrona, el productor la captura sin romper la transacción
        when(template.send(anyString(), anyString(), any())).thenThrow(new RuntimeException("Broker down"));
        assertDoesNotThrow(() -> producer.publicarEventoOrden(orderDto, true));
    }

    @Test
    @DisplayName("OrderKafkaPartitioner aplica Round-Robin sin key y hash fijo con la misma key")
    void partitionerRoundRobinAndStickyByKey() {
        OrderKafkaPartitioner partitioner = new OrderKafkaPartitioner();
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = List.of(
                new PartitionInfo(OrderKafkaController.TOPIC_ORDENES, 0, node, new Node[]{node}, new Node[]{node}),
                new PartitionInfo(OrderKafkaController.TOPIC_ORDENES, 1, node, new Node[]{node}, new Node[]{node}),
                new PartitionInfo(OrderKafkaController.TOPIC_ORDENES, 2, node, new Node[]{node}, new Node[]{node})
        );
        Cluster cluster = new Cluster("test-cluster", List.of(node), partitions, Collections.emptySet(), Collections.emptySet());

        assertEquals(0, partitioner.partition(OrderKafkaController.TOPIC_ORDENES, null, null, "A", new byte[]{1}, cluster));
        assertEquals(1, partitioner.partition(OrderKafkaController.TOPIC_ORDENES, null, null, "B", new byte[]{2}, cluster));
        assertEquals(2, partitioner.partition(OrderKafkaController.TOPIC_ORDENES, null, null, "C", new byte[]{3}, cluster));

        byte[] keyBytes = "orden-555".getBytes(StandardCharsets.UTF_8);
        int p1 = partitioner.partition(OrderKafkaController.TOPIC_ORDENES, "orden-555", keyBytes, "P1", new byte[]{1}, cluster);
        int p2 = partitioner.partition(OrderKafkaController.TOPIC_ORDENES, "orden-555", keyBytes, "P2", new byte[]{2}, cluster);
        assertEquals(p1, p2);
    }
}
