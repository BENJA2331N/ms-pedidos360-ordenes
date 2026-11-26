package com.pedidos360.orders.kafka;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderKafkaController.class)
class OrderKafkaTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("GET /api/kafka/send sin key envía al topic con distribución automática")
    void sendWithoutKey() throws Exception {
        mockMvc.perform(get("/api/kafka/send").param("message", "PedidoNuevo-ClienteA"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("SIN KEY")))
                .andExpect(content().string(containsString("PedidoNuevo-ClienteA")));

        verify(kafkaTemplate).send(OrderKafkaController.TOPIC_NAME, "PedidoNuevo-ClienteA");
    }

    @Test
    @DisplayName("GET /api/kafka/send con key envía al topic garantizando misma partición por clave")
    void sendWithKey() throws Exception {
        mockMvc.perform(get("/api/kafka/send")
                        .param("message", "Paso1-OrdenCreada")
                        .param("key", "orden-555"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("CON KEY: orden-555")))
                .andExpect(content().string(containsString("Paso1-OrdenCreada")));

        verify(kafkaTemplate).send(OrderKafkaController.TOPIC_NAME, "orden-555", "Paso1-OrdenCreada");
    }

    @Test
    @DisplayName("GET /api/kafka/health responde estado operativo")
    void healthEndpoint() throws Exception {
        mockMvc.perform(get("/api/kafka/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("Módulo de Streaming Kafka en Pedidos360 operativo"));
    }

    @Test
    @DisplayName("OrderKafkaConsumer imprime Partición, Offset, Key y Mensaje en consola")
    void consumerPrintsMetadata() {
        OrderKafkaConsumer consumer = new OrderKafkaConsumer();
        PrintStream originalOut = System.out;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
            consumer.listen("Paso1-OrdenCreada", 1, 4L, "orden-555");
        } finally {
            System.setOut(originalOut);
        }

        String output = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("Partición : 1"));
        assertTrue(output.contains("Offset    : 4"));
        assertTrue(output.contains("Key       : orden-555"));
        assertTrue(output.contains("Mensaje   : Paso1-OrdenCreada"));
    }

    @Test
    @DisplayName("OrderKafkaPartitioner aplica Round-Robin sin key y hash fijo con la misma key")
    void partitionerRoundRobinAndStickyByKey() {
        OrderKafkaPartitioner partitioner = new OrderKafkaPartitioner();
        Node node = new Node(1, "localhost", 9092);
        List<PartitionInfo> partitions = List.of(
                new PartitionInfo(OrderKafkaController.TOPIC_NAME, 0, node, new Node[]{node}, new Node[]{node}),
                new PartitionInfo(OrderKafkaController.TOPIC_NAME, 1, node, new Node[]{node}, new Node[]{node}),
                new PartitionInfo(OrderKafkaController.TOPIC_NAME, 2, node, new Node[]{node}, new Node[]{node})
        );
        Cluster cluster = new Cluster("test-cluster", List.of(node), partitions, Collections.emptySet(), Collections.emptySet());

        // Sin key: 0 -> 1 -> 2
        assertEquals(0, partitioner.partition(OrderKafkaController.TOPIC_NAME, null, null, "A", new byte[]{1}, cluster));
        assertEquals(1, partitioner.partition(OrderKafkaController.TOPIC_NAME, null, null, "B", new byte[]{2}, cluster));
        assertEquals(2, partitioner.partition(OrderKafkaController.TOPIC_NAME, null, null, "C", new byte[]{3}, cluster));

        // Con la misma key ("orden-555"): siempre la misma partición
        byte[] keyBytes = "orden-555".getBytes(StandardCharsets.UTF_8);
        int p1 = partitioner.partition(OrderKafkaController.TOPIC_NAME, "orden-555", keyBytes, "Paso1", new byte[]{1}, cluster);
        int p2 = partitioner.partition(OrderKafkaController.TOPIC_NAME, "orden-555", keyBytes, "Paso2", new byte[]{2}, cluster);
        int p3 = partitioner.partition(OrderKafkaController.TOPIC_NAME, "orden-555", keyBytes, "Paso3", new byte[]{3}, cluster);
        assertEquals(p1, p2);
        assertEquals(p2, p3);
    }
}
