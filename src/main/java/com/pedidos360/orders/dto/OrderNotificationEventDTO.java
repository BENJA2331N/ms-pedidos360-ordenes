package com.pedidos360.orders.dto;

public class OrderNotificationEventDTO {
    private Long orderId;
    private String status;
    private String mensaje;

    public OrderNotificationEventDTO() {}

    public OrderNotificationEventDTO(Long orderId, String status, String mensaje) {
        this.orderId = orderId;
        this.status = status;
        this.mensaje = mensaje;
    }

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getMensaje() { return mensaje; }
    public void setMensaje(String mensaje) { this.mensaje = mensaje; }

    @Override
    public String toString() {
        return "OrderNotificationEventDTO{orderId=" + orderId + ", status='" + status + "', mensaje='" + mensaje + "'}";
    }
}
