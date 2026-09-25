package com.pedidos360.orders.dto;

import com.pedidos360.orders.model.Order;
import com.pedidos360.orders.model.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class OrderResponseDTO {

    private Long id;
    private String clienteId;
    private OrderStatus estado;
    private LocalDateTime fechaCreacion;
    private List<OrderItemResponseDTO> items;
    private BigDecimal total;

    public OrderResponseDTO() {
    }

    public OrderResponseDTO(Long id, String clienteId, OrderStatus estado, LocalDateTime fechaCreacion, List<OrderItemResponseDTO> items, BigDecimal total) {
        this.id = id;
        this.clienteId = clienteId;
        this.estado = estado;
        this.fechaCreacion = fechaCreacion;
        this.items = items;
        this.total = total;
    }

    public static OrderResponseDTO fromEntity(Order order) {
        if (order == null) {
            return null;
        }
        List<OrderItemResponseDTO> itemDTOs = order.getItems() != null
                ? order.getItems().stream().map(OrderItemResponseDTO::fromEntity).toList()
                : Collections.emptyList();

        return new OrderResponseDTO(
                order.getId(),
                order.getClienteId(),
                order.getEstado(),
                order.getFechaCreacion(),
                itemDTOs,
                order.getTotal()
        );
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getClienteId() {
        return clienteId;
    }

    public void setClienteId(String clienteId) {
        this.clienteId = clienteId;
    }

    public OrderStatus getEstado() {
        return estado;
    }

    public void setEstado(OrderStatus estado) {
        this.estado = estado;
    }

    public LocalDateTime getFechaCreacion() {
        return fechaCreacion;
    }

    public void setFechaCreacion(LocalDateTime fechaCreacion) {
        this.fechaCreacion = fechaCreacion;
    }

    public List<OrderItemResponseDTO> getItems() {
        return items;
    }

    public void setItems(List<OrderItemResponseDTO> items) {
        this.items = items;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }
}
