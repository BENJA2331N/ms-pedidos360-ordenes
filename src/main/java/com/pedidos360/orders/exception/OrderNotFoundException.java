package com.pedidos360.orders.exception;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(Long id) {
        super("Pedido con ID " + id + " no encontrado.");
    }

    public OrderNotFoundException(String message) {
        super(message);
    }
}
