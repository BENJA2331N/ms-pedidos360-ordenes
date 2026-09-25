package com.pedidos360.orders.controller;

import com.pedidos360.orders.dto.OrderRequestDTO;
import com.pedidos360.orders.dto.OrderResponseDTO;
import com.pedidos360.orders.dto.StatusUpdateDTO;
import com.pedidos360.orders.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@CrossOrigin(origins = "*")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Listar todos los pedidos.
     * Rol esperado: Operador, Admin
     */
    @GetMapping
    public ResponseEntity<List<OrderResponseDTO>> getAllOrders() {
        List<OrderResponseDTO> orders = orderService.findAll();
        return ResponseEntity.ok(orders);
    }

    /**
     * Listar pedidos propios del cliente autenticado.
     * Rol esperado: Cliente (identificado mediante token propagado por el BFF en X-User-Id / X-Cliente-Id)
     */
    @GetMapping("/mine")
    public ResponseEntity<List<OrderResponseDTO>> getMyOrders(
            @RequestHeader(value = "X-User-Id", required = false) String xUserId,
            @RequestHeader(value = "X-Cliente-Id", required = false) String xClienteId,
            @RequestParam(value = "clienteId", required = false) String clienteIdParam) {

        String clienteId = resolveClienteId(xUserId, xClienteId, clienteIdParam);
        List<OrderResponseDTO> orders = orderService.findByCliente(clienteId);
        return ResponseEntity.ok(orders);
    }

    /**
     * Obtener detalle de un pedido por ID.
     * Rol esperado: Cliente, Operador, Admin
     */
    @GetMapping("/{id}")
    public ResponseEntity<OrderResponseDTO> getOrderById(@PathVariable Long id) {
        OrderResponseDTO order = orderService.findById(id);
        return ResponseEntity.ok(order);
    }

    /**
     * Crear un nuevo pedido.
     * Rol esperado: Cliente
     */
    @PostMapping
    public ResponseEntity<OrderResponseDTO> createOrder(
            @Valid @RequestBody OrderRequestDTO dto,
            @RequestHeader(value = "X-User-Id", required = false) String xUserId,
            @RequestHeader(value = "X-Cliente-Id", required = false) String xClienteId) {

        String clienteId = resolveClienteId(xUserId, xClienteId, dto.getClienteId());
        OrderResponseDTO created = orderService.create(dto, clienteId);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    /**
     * Cambiar el estado de un pedido según la máquina de estados.
     * Rol esperado: Operador, Admin
     */
    @PatchMapping("/{id}/status")
    public ResponseEntity<OrderResponseDTO> updateOrderStatus(
            @PathVariable Long id,
            @Valid @RequestBody StatusUpdateDTO dto) {

        OrderResponseDTO updated = orderService.updateStatus(id, dto.getNuevoEstado());
        return ResponseEntity.ok(updated);
    }

    private String resolveClienteId(String xUserId, String xClienteId, String fallbackId) {
        if (xUserId != null && !xUserId.trim().isEmpty()) {
            return xUserId.trim();
        }
        if (xClienteId != null && !xClienteId.trim().isEmpty()) {
            return xClienteId.trim();
        }
        if (fallbackId != null && !fallbackId.trim().isEmpty()) {
            return fallbackId.trim();
        }
        throw new IllegalArgumentException(
                "Identificador de cliente no provisto. Se requiere el encabezado 'X-User-Id' o 'X-Cliente-Id' propagado por el BFF."
        );
    }
}
