package com.pedidos360.orders.exception;

import com.pedidos360.orders.model.OrderStatus;

public class InvalidStatusTransitionException extends RuntimeException {

    private final OrderStatus estadoActual;
    private final OrderStatus nuevoEstado;

    public InvalidStatusTransitionException(OrderStatus estadoActual, OrderStatus nuevoEstado) {
        super(String.format("Transición de estado inválida: no es posible cambiar el estado de '%s' a '%s'.",
                estadoActual, nuevoEstado));
        this.estadoActual = estadoActual;
        this.nuevoEstado = nuevoEstado;
    }

    public InvalidStatusTransitionException(String message) {
        super(message);
        this.estadoActual = null;
        this.nuevoEstado = null;
    }

    public OrderStatus getEstadoActual() {
        return estadoActual;
    }

    public OrderStatus getNuevoEstado() {
        return nuevoEstado;
    }
}
