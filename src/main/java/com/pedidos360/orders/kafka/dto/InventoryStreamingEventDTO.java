package com.pedidos360.orders.kafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.UUID;

/**
 * DTO estructurado y validado para el Tópico 2 de Kafka (pedidos360.inventario.eventos).
 * Representa movimientos de stock y catálogo compartidos entre ms-catalogo y ms-ordenes.
 */
public class InventoryStreamingEventDTO {

    @NotBlank(message = "El eventId es obligatorio")
    private String eventId;

    @NotNull(message = "El productId es obligatorio")
    @Positive(message = "El productId debe ser mayor a 0")
    private Long productId;

    @NotBlank(message = "El nombre del producto es obligatorio")
    private String productoNombre;

    @NotBlank(message = "El tipoMovimiento es obligatorio")
    private String tipoMovimiento;

    @NotNull(message = "La cantidadMovida es obligatoria")
    @Min(value = 0, message = "La cantidadMovida no puede ser negativa")
    private Integer cantidadMovida;

    @NotNull(message = "El stockRestante es obligatorio")
    @Min(value = 0, message = "El stockRestante no puede ser negativo")
    private Integer stockRestante;

    @NotBlank(message = "El timestamp es obligatorio")
    private String timestamp;

    private String origen;

    public InventoryStreamingEventDTO() {
    }

    public InventoryStreamingEventDTO(Long productId, String productoNombre, String tipoMovimiento,
                                      Integer cantidadMovida, Integer stockRestante, String origen) {
        this.eventId = UUID.randomUUID().toString();
        this.productId = productId;
        this.productoNombre = productoNombre;
        this.tipoMovimiento = tipoMovimiento;
        this.cantidadMovida = cantidadMovida;
        this.stockRestante = stockRestante;
        this.timestamp = Instant.now().toString();
        this.origen = origen;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public Long getProductId() { return productId; }
    public void setProductId(Long productId) { this.productId = productId; }
    public String getProductoNombre() { return productoNombre; }
    public void setProductoNombre(String productoNombre) { this.productoNombre = productoNombre; }
    public String getTipoMovimiento() { return tipoMovimiento; }
    public void setTipoMovimiento(String tipoMovimiento) { this.tipoMovimiento = tipoMovimiento; }
    public Integer getCantidadMovida() { return cantidadMovida; }
    public void setCantidadMovida(Integer cantidadMovida) { this.cantidadMovida = cantidadMovida; }
    public Integer getStockRestante() { return stockRestante; }
    public void setStockRestante(Integer stockRestante) { this.stockRestante = stockRestante; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public String getOrigen() { return origen; }
    public void setOrigen(String origen) { this.origen = origen; }

    @Override
    public String toString() {
        return "InventoryStreamingEventDTO{" +
                "eventId='" + eventId + '\'' +
                ", productId=" + productId +
                ", productoNombre='" + productoNombre + '\'' +
                ", tipoMovimiento='" + tipoMovimiento + '\'' +
                ", cantidadMovida=" + cantidadMovida +
                ", stockRestante=" + stockRestante +
                ", timestamp='" + timestamp + '\'' +
                ", origen='" + origen + '\'' +
                '}';
    }
}
