package com.pedidos360.orders.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.orders.dto.*;
import com.pedidos360.orders.exception.GlobalExceptionHandler;
import com.pedidos360.orders.exception.InvalidStatusTransitionException;
import com.pedidos360.orders.exception.OrderNotFoundException;
import com.pedidos360.orders.model.OrderStatus;
import com.pedidos360.orders.service.OrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
@Import(GlobalExceptionHandler.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderService orderService;

    @Test
    @DisplayName("GET /api/orders: debe listar todos los pedidos con 200 OK")
    void testGetAllOrders() throws Exception {
        OrderResponseDTO dto = new OrderResponseDTO(
                1L, "user-1", OrderStatus.CREADO, LocalDateTime.now(), List.of(), new BigDecimal("100.00")
        );
        when(orderService.findAll()).thenReturn(List.of(dto));

        mockMvc.perform(get("/api/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(1))
                .andExpect(jsonPath("$[0].clienteId").value("user-1"))
                .andExpect(jsonPath("$[0].estado").value("CREADO"));
    }

    @Test
    @DisplayName("GET /api/orders/mine: debe obtener pedidos del usuario autenticado vía cabecera X-User-Id")
    void testGetMyOrders() throws Exception {
        OrderResponseDTO dto = new OrderResponseDTO(
                2L, "cliente-777", OrderStatus.ACEPTADO, LocalDateTime.now(), List.of(), new BigDecimal("250.00")
        );
        when(orderService.findByCliente("cliente-777")).thenReturn(List.of(dto));

        mockMvc.perform(get("/api/orders/mine")
                        .header("X-User-Id", "cliente-777"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(2))
                .andExpect(jsonPath("$[0].clienteId").value("cliente-777"));
    }

    @Test
    @DisplayName("POST /api/orders: debe crear pedido con 201 CREATED")
    void testCreateOrder() throws Exception {
        OrderItemRequestDTO item = new OrderItemRequestDTO(5L, 3, new BigDecimal("120.00"));
        OrderRequestDTO request = new OrderRequestDTO(List.of(item));

        OrderResponseDTO responseDto = new OrderResponseDTO(
                10L, "cliente-abc", OrderStatus.CREADO, LocalDateTime.now(),
                List.of(new OrderItemResponseDTO(1L, 5L, 3, new BigDecimal("120.00"), new BigDecimal("360.00"))),
                new BigDecimal("360.00")
        );

        when(orderService.create(any(OrderRequestDTO.class), eq("cliente-abc"))).thenReturn(responseDto);

        mockMvc.perform(post("/api/orders")
                        .header("X-User-Id", "cliente-abc")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(10))
                .andExpect(jsonPath("$.clienteId").value("cliente-abc"))
                .andExpect(jsonPath("$.total").value(360.00));
    }

    @Test
    @DisplayName("PATCH /api/orders/{id}/status: debe actualizar estado con 200 OK")
    void testUpdateOrderStatus() throws Exception {
        StatusUpdateDTO updateDto = new StatusUpdateDTO(OrderStatus.ACEPTADO);

        OrderResponseDTO responseDto = new OrderResponseDTO(
                1L, "cliente-abc", OrderStatus.ACEPTADO, LocalDateTime.now(), List.of(), new BigDecimal("100.00")
        );

        when(orderService.updateStatus(1L, OrderStatus.ACEPTADO)).thenReturn(responseDto);

        mockMvc.perform(patch("/api/orders/1/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.estado").value("ACEPTADO"));
    }

    @Test
    @DisplayName("PATCH /api/orders/{id}/status con transición inválida retorna 400 Bad Request")
    void testUpdateOrderStatus_InvalidTransition() throws Exception {
        StatusUpdateDTO updateDto = new StatusUpdateDTO(OrderStatus.ENTREGADO);

        when(orderService.updateStatus(1L, OrderStatus.ENTREGADO))
                .thenThrow(new InvalidStatusTransitionException(OrderStatus.CREADO, OrderStatus.ENTREGADO));

        mockMvc.perform(patch("/api/orders/1/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Transición de estado inválida")));
    }

    @Test
    @DisplayName("GET /api/orders/{id} con ID inexistente retorna 404 Not Found")
    void testGetOrder_NotFound() throws Exception {
        when(orderService.findById(999L)).thenThrow(new OrderNotFoundException(999L));

        mockMvc.perform(get("/api/orders/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }
}
