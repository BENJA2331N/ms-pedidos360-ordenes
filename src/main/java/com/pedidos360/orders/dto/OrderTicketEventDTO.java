package com.pedidos360.orders.dto;

import java.math.BigDecimal;

/**
 * Evento asíncrono para la generación de tickets y comprobantes de pedidos.
 */
public class OrderTicketEventDTO {

    private Long orderId;
    private String clienteId;
    private String estado;
    private BigDecimal total;
    private Integer cantidadItems;
    private String detalle;

    public OrderTicketEventDTO() {
    }

    public OrderTicketEventDTO(Long orderId, String clienteId, String estado, BigDecimal total, Integer cantidadItems, String detalle) {
        this.orderId = orderId;
        this.clienteId = clienteId;
        this.estado = estado;
        this.total = total;
        this.cantidadItems = cantidadItems;
        this.detalle = detalle;
    }

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
    public String getDetalle() { return detalle; }
    public void setDetalle(String detalle) { this.detalle = detalle; }

    @Override
    public String toString() {
        return "OrderTicketEventDTO{orderId=" + orderId
                + ", clienteId='" + clienteId + '\''
                + ", estado='" + estado + '\''
                + ", total=" + total
                + ", cantidadItems=" + cantidadItems
                + ", detalle='" + detalle + '\''
                + '}';
    }
}
