package com.pedidos360.orders.kafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * DTO estructurado y validado para el Tópico 1 de Kafka (pedidos360.ordenes.eventos).
 * Representa un evento inmutable del ciclo de vida de un pedido para analítica y auditoría.
 */
public class OrderStreamingEventDTO {

    @NotBlank(message = "El eventId es obligatorio")
    private String eventId;

    @NotNull(message = "El orderId es obligatorio")
    @Positive(message = "El orderId debe ser mayor a 0")
    private Long orderId;

    @NotBlank(message = "El clienteId es obligatorio")
    private String clienteId;

    @NotBlank(message = "El estado del pedido es obligatorio")
    private String estado;

    @NotNull(message = "El monto total es obligatorio")
    @Min(value = 0, message = "El monto total no puede ser negativo")
    private BigDecimal total;

    @NotNull(message = "La cantidad de ítems es obligatoria")
    @Positive(message = "La cantidad de ítems debe ser al menos 1")
    private Integer cantidadItems;

    @NotBlank(message = "El timestamp es obligatorio")
    private String timestamp;

    private String origen;

    public OrderStreamingEventDTO() {
    }

    public OrderStreamingEventDTO(Long orderId, String clienteId, String estado,
                                  BigDecimal total, Integer cantidadItems, String origen) {
        this.eventId = UUID.randomUUID().toString();
        this.orderId = orderId;
        this.clienteId = clienteId;
        this.estado = estado;
        this.total = total;
        this.cantidadItems = cantidadItems;
        this.timestamp = Instant.now().toString();
        this.origen = origen;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    public String getClienteId() { return clienteId; }
    public void setClienteId(String clienteId) { this.clienteId = clienteId; }
    public String getEstado() { return estado; }
    public void setEstado(String estado) { this.estado = estado; }
    public BigDecimal getTotal() { return total; }
    public void setTotal(BigDecimal total) { this.total = total; }
    public Integer getCantidadItems() { return cantidadItems; }
    public void setCantidadItems(Integer cantidadItems) { this.cantidadItems = cantidadItems; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public String getOrigen() { return origen; }
    public void setOrigen(String origen) { this.origen = origen; }

    @Override
    public String toString() {
        return "OrderStreamingEventDTO{" +
                "eventId='" + eventId + '\'' +
                ", orderId=" + orderId +
                ", clienteId='" + clienteId + '\'' +
                ", estado='" + estado + '\'' +
                ", total=" + total +
                ", cantidadItems=" + cantidadItems +
                ", timestamp='" + timestamp + '\'' +
                ", origen='" + origen + '\'' +
                '}';
    }
}
