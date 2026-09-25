package com.pedidos360.orders.dto;

import com.pedidos360.orders.model.OrderStatus;
import jakarta.validation.constraints.NotNull;

public class StatusUpdateDTO {

    @NotNull(message = "El nuevo estado es obligatorio")
    private OrderStatus nuevoEstado;

    public StatusUpdateDTO() {
    }

    public StatusUpdateDTO(OrderStatus nuevoEstado) {
        this.nuevoEstado = nuevoEstado;
    }

    public OrderStatus getNuevoEstado() {
        return nuevoEstado;
    }

    public void setNuevoEstado(OrderStatus nuevoEstado) {
        this.nuevoEstado = nuevoEstado;
    }

    // Permite que clientes que envíen { "estado": "ACEPTADO" } también funcionen
    public void setEstado(OrderStatus estado) {
        this.nuevoEstado = estado;
    }
}
