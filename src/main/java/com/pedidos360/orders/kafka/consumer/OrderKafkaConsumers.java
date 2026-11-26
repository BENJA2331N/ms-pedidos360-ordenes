package com.pedidos360.orders.kafka.consumer;

import com.pedidos360.orders.kafka.domain.OrderStreamingDomainService;
import com.pedidos360.orders.kafka.dto.InventoryStreamingEventDTO;
import com.pedidos360.orders.kafka.dto.OrderStreamingEventDTO;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Capa de infraestructura de consumidores Kafka (@KafkaListener).
 * Valida los DTOs entrantes, registra contexto completo (tópico, partición, offset, key, grupo)
 * y delega la lógica de negocio en {@link OrderStreamingDomainService}.
 */
@Component
public class OrderKafkaConsumers {

    private static final Logger log = LoggerFactory.getLogger(OrderKafkaConsumers.class);

    private final OrderStreamingDomainService domainService;
    private final Validator validator;

    public OrderKafkaConsumers(OrderStreamingDomainService domainService, Validator validator) {
        this.domainService = domainService;
        this.validator = validator;
    }

    /**
     * Consumer Group 1 (Analítica) con concurrencia = 3 (1 hilo por partición para balanceo de carga).
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ordenes-eventos.name:pedidos360.ordenes.eventos}",
            groupId = "${app.kafka.groups.analitica:pedidos360-ordenes-analitica-group}",
            containerFactory = "orderKafkaListenerContainerFactory",
            concurrency = "3"
    )
    public void consumirOrdenParaAnalitica(
            @Payload(required = false) OrderStreamingEventDTO dto,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER:ANALITICA] Recibido en topic={} partition={} offset={} key={} payload={}",
                topic, partition, offset, key != null ? key : "(sin key)", dto);
        validarPayload(dto, "OrderStreamingEventDTO");
        domainService.procesarAnaliticaOrden(dto);
    }

    /**
     * Consumer Group 2 (Auditoría) independiente sobre el mismo Tópico 1 (división de tareas).
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ordenes-eventos.name:pedidos360.ordenes.eventos}",
            groupId = "${app.kafka.groups.auditoria:pedidos360-ordenes-auditoria-group}",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consumirOrdenParaAuditoria(
            @Payload(required = false) OrderStreamingEventDTO dto,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER:AUDITORIA] Recibido en topic={} partition={} offset={} key={} payload={}",
                topic, partition, offset, key != null ? key : "(sin key)", dto);
        validarPayload(dto, "OrderStreamingEventDTO");
        domainService.registrarAuditoriaOrden(dto);
    }

    /**
     * Consumer Group 3 (Sincronización de Inventario) escuchando el Tópico 2 (pedidos360.inventario.eventos).
     */
    @KafkaListener(
            topics = "${app.kafka.topics.inventario-eventos.name:pedidos360.inventario.eventos}",
            groupId = "${app.kafka.groups.inventario-sync:pedidos360-ordenes-inventario-sync-group}",
            containerFactory = "inventoryKafkaListenerContainerFactory"
    )
    public void consumirEventoInventario(
            @Payload(required = false) InventoryStreamingEventDTO dto,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("[KAFKA-CONSUMER:INVENTARIO-SYNC] Recibido en topic={} partition={} offset={} key={} payload={}",
                topic, partition, offset, key != null ? key : "(sin key)", dto);
        validarPayload(dto, "InventoryStreamingEventDTO");
        domainService.sincronizarProyeccionInventario(dto);
    }

    /**
     * Consumidor del Dead Letter Topic (DLT) para auditar en logs mensajes corruptos o agotados.
     */
    @KafkaListener(
            topics = "${app.kafka.topics.ordenes-eventos.dlt:pedidos360.ordenes.eventos.DLT}",
            groupId = "${app.kafka.groups.dlt:pedidos360-ordenes-dlt-group}",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void registrarMensajeEnDlt(
            @Payload(required = false) String rawPayload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.error("[KAFKA-DLT-ORDENES] Evento fallido capturado en DLT | topic={} partition={} offset={} key={} payload={}",
                topic, partition, offset, key != null ? key : "(sin key)", rawPayload);
    }

    private <T> void validarPayload(T dto, String nombreTipo) {
        if (dto == null) {
            throw new IllegalArgumentException(nombreTipo + " nulo recibido desde Kafka");
        }
        Set<ConstraintViolation<T>> violaciones = validator.validate(dto);
        if (!violaciones.isEmpty()) {
            String detalle = violaciones.stream()
                    .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                    .collect(Collectors.joining(", "));
            throw new IllegalArgumentException("Payload inválido en " + nombreTipo + " -> " + detalle);
        }
    }
}
