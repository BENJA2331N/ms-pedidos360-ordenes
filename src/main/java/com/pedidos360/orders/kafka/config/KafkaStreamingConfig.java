package com.pedidos360.orders.kafka.config;

import com.pedidos360.orders.kafka.OrderKafkaPartitioner;
import com.pedidos360.orders.kafka.dto.InventoryStreamingEventDTO;
import com.pedidos360.orders.kafka.dto.OrderStreamingEventDTO;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuración centralizada de Apache Kafka para ms-pedidos360-ordenes:
 * 1) Gobernanza declarativa de los 2 tópicos (particiones, réplicas, retención y cleanup.policy) + DLTs.
 * 2) Serialización JSON portable (sin cabeceras acopladas de paquete Java) + soporte byte[] para DLT.
 * 3) Tolerancia a fallos con ErrorHandlingDeserializer, reintentos con FixedBackOff y envío a DLT.
 */
@Configuration
public class KafkaStreamingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamingConfig.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.ordenes-eventos.name:pedidos360.ordenes.eventos}")
    private String ordenesTopic;

    @Value("${app.kafka.topics.ordenes-eventos.dlt:pedidos360.ordenes.eventos.DLT}")
    private String ordenesDltTopic;

    @Value("${app.kafka.topics.ordenes-eventos.partitions:3}")
    private int ordenesPartitions;

    @Value("${app.kafka.topics.ordenes-eventos.replicas:1}")
    private int ordenesReplicas;

    @Value("${app.kafka.topics.ordenes-eventos.retention-ms:604800000}")
    private String ordenesRetentionMs;

    @Value("${app.kafka.topics.ordenes-eventos.cleanup-policy:delete}")
    private String ordenesCleanupPolicy;

    @Value("${app.kafka.topics.inventario-eventos.name:pedidos360.inventario.eventos}")
    private String inventarioTopic;

    @Value("${app.kafka.topics.inventario-eventos.dlt:pedidos360.inventario.eventos.DLT}")
    private String inventarioDltTopic;

    @Value("${app.kafka.topics.inventario-eventos.partitions:3}")
    private int inventarioPartitions;

    @Value("${app.kafka.topics.inventario-eventos.replicas:1}")
    private int inventarioReplicas;

    @Value("${app.kafka.topics.inventario-eventos.retention-ms:259200000}")
    private String inventarioRetentionMs;

    @Value("${app.kafka.topics.inventario-eventos.cleanup-policy:compact,delete}")
    private String inventarioCleanupPolicy;

    // =========================================================================
    // 1. DEFINICIÓN DE LOS 2 TÓPICOS Y SUS DLTs (GOBERNANZA DE PARÁMETROS)
    // =========================================================================

    @Bean
    public NewTopic ordenesEventosTopic() {
        return TopicBuilder.name(ordenesTopic)
                .partitions(ordenesPartitions)
                .replicas(ordenesReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, ordenesRetentionMs)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, ordenesCleanupPolicy)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    @Bean
    public NewTopic ordenesEventosDltTopic() {
        return TopicBuilder.name(ordenesDltTopic)
                .partitions(ordenesPartitions)
                .replicas(ordenesReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000") // 14 días auditoría de errores
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    @Bean
    public NewTopic inventarioEventosTopic() {
        return TopicBuilder.name(inventarioTopic)
                .partitions(inventarioPartitions)
                .replicas(inventarioReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, inventarioRetentionMs)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, inventarioCleanupPolicy)
                .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5")
                .build();
    }

    @Bean
    public NewTopic inventarioEventosDltTopic() {
        return TopicBuilder.name(inventarioDltTopic)
                .partitions(inventarioPartitions)
                .replicas(inventarioReplicas)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    // =========================================================================
    // 2. PRODUCTOR TIPADO (DTOs JSON + byte[] PARA PAYLOADS CORRUPTOS EN DLT)
    // =========================================================================

    @Bean
    public ProducerFactory<String, Object> kafkaProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OrderKafkaPartitioner.class);

        JsonSerializer<Object> jsonSerializer = new JsonSerializer<>();
        jsonSerializer.setAddTypeInfo(false);

        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, jsonSerializer);

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new DelegatingByTypeSerializer(delegates, true));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> kafkaProducerFactory) {
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    // =========================================================================
    // 3. MANEJO DE ERRORES, REINTENTOS Y DEAD LETTER TOPIC (DLT)
    // =========================================================================

    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("[KAFKA-DLT-RECOVERER] Enviando mensaje fallido a {}.DLT | topic={} partition={} offset={} key={} causa={}",
                            record.topic(), record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage());
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                }
        );

        // 2 reintentos con espera de 1 segundo para fallos transitorios
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        // Errores de validación o deserialización no son recuperables: van directo al DLT sin reintentar
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class, DeserializationException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("[KAFKA-RETRY] Intento #{} para mensaje en topic={} partition={} offset={} key={} motivo={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage())
        );
        return errorHandler;
    }

    // =========================================================================
    // 4. FACTORIES DE CONSUMIDORES POR DTO Y DLT
    // =========================================================================

    @Bean
    public ConsumerFactory<String, OrderStreamingEventDTO> orderConsumerFactory() {
        JsonDeserializer<OrderStreamingEventDTO> jsonDeserializer = new JsonDeserializer<>(OrderStreamingEventDTO.class, false);
        jsonDeserializer.addTrustedPackages("*");
        ErrorHandlingDeserializer<OrderStreamingEventDTO> valueDeserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);
        return new DefaultKafkaConsumerFactory<>(baseConsumerConfigs(), new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderStreamingEventDTO> orderKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderStreamingEventDTO> orderConsumerFactory,
            CommonErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderStreamingEventDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderConsumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, InventoryStreamingEventDTO> inventoryConsumerFactory() {
        JsonDeserializer<InventoryStreamingEventDTO> jsonDeserializer = new JsonDeserializer<>(InventoryStreamingEventDTO.class, false);
        jsonDeserializer.addTrustedPackages("*");
        ErrorHandlingDeserializer<InventoryStreamingEventDTO> valueDeserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);
        return new DefaultKafkaConsumerFactory<>(baseConsumerConfigs(), new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryStreamingEventDTO> inventoryKafkaListenerContainerFactory(
            ConsumerFactory<String, InventoryStreamingEventDTO> inventoryConsumerFactory,
            CommonErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, InventoryStreamingEventDTO> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(inventoryConsumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dltKafkaListenerContainerFactory() {
        ConsumerFactory<String, String> dltFactory = new DefaultKafkaConsumerFactory<>(
                baseConsumerConfigs(), new StringDeserializer(), new StringDeserializer());
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dltFactory);
        return factory;
    }

    private Map<String, Object> baseConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }
}
