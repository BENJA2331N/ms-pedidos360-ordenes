package com.pedidos360.orders.service;

import com.pedidos360.orders.dto.OrderItemRequestDTO;
import com.pedidos360.orders.dto.OrderRequestDTO;
import com.pedidos360.orders.dto.OrderResponseDTO;
import com.pedidos360.orders.exception.InvalidStatusTransitionException;
import com.pedidos360.orders.exception.OrderNotFoundException;
import com.pedidos360.orders.model.Order;
import com.pedidos360.orders.model.OrderItem;
import com.pedidos360.orders.model.OrderStatus;
import com.pedidos360.orders.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private CatalogClient catalogClient;

    @Mock
    private com.pedidos360.orders.messaging.producer.OrderEventProducer orderEventProducer;

    @InjectMocks
    private OrderService orderService;

    private Order testOrder;

    @BeforeEach
    void setUp() {
        testOrder = new Order();
        testOrder.setId(1L);
        testOrder.setClienteId("client-uuid-123");
        testOrder.setEstado(OrderStatus.CREADO);
        testOrder.setFechaCreacion(LocalDateTime.now());

        OrderItem item1 = new OrderItem(10L, 2, new BigDecimal("150.00"));
        OrderItem item2 = new OrderItem(20L, 1, new BigDecimal("300.00"));
        testOrder.addItem(item1);
        testOrder.addItem(item2);
        testOrder.calcularTotal();
    }

    @Test
    @DisplayName("Debe crear un pedido con estado inicial CREADO y calcular el total correctamente")
    void testCreateOrder_Success() {
        OrderItemRequestDTO itemDto1 = new OrderItemRequestDTO(10L, 2, new BigDecimal("150.00"));
        OrderItemRequestDTO itemDto2 = new OrderItemRequestDTO(20L, 1, new BigDecimal("300.00"));
        OrderRequestDTO request = new OrderRequestDTO(List.of(itemDto1, itemDto2));

        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order o = invocation.getArgument(0);
            o.setId(99L);
            return o;
        });

        OrderResponseDTO response = orderService.create(request, "client-uuid-123");

        assertNotNull(response);
        assertEquals(99L, response.getId());
        assertEquals("client-uuid-123", response.getClienteId());
        assertEquals(OrderStatus.CREADO, response.getEstado());
        assertEquals(new BigDecimal("600.00"), response.getTotal());
        assertEquals(2, response.getItems().size());
        verify(orderRepository).save(any(Order.class));
    }

    @Test
    @DisplayName("Debe lanzar excepción al crear pedido sin clienteId")
    void testCreateOrder_MissingClienteId() {
        OrderRequestDTO request = new OrderRequestDTO(List.of(new OrderItemRequestDTO(1L, 1, BigDecimal.TEN)));
        assertThrows(IllegalArgumentException.class, () -> orderService.create(request, ""));
    }

    @Test
    @DisplayName("Debe listar pedidos por cliente")
    void testFindByCliente() {
        when(orderRepository.findByClienteIdOrderByFechaCreacionDesc("client-uuid-123"))
                .thenReturn(List.of(testOrder));

        List<OrderResponseDTO> result = orderService.findByCliente("client-uuid-123");

        assertEquals(1, result.size());
        assertEquals("client-uuid-123", result.get(0).getClienteId());
    }

    @Test
    @DisplayName("Transición CREADO -> ACEPTADO: debe llamar a CatalogClient para descontar stock de cada item")
    void testTransition_CreadoToAceptado_CallsCatalog() {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        OrderResponseDTO updated = orderService.updateStatus(1L, OrderStatus.ACEPTADO);

        assertEquals(OrderStatus.ACEPTADO, updated.getEstado());
        verify(catalogClient).descontarStock(10L, 2);
        verify(catalogClient).descontarStock(20L, 1);
        verify(orderRepository).save(testOrder);
    }

    @Test
    @DisplayName("Transición CREADO -> CANCELADO: permitida sin llamar a catálogo")
    void testTransition_CreadoToCancelado() {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        OrderResponseDTO updated = orderService.updateStatus(1L, OrderStatus.CANCELADO);

        assertEquals(OrderStatus.CANCELADO, updated.getEstado());
        verifyNoInteractions(catalogClient);
        verify(orderRepository).save(testOrder);
    }

    @Test
    @DisplayName("Transición ACEPTADO -> EN_PREPARACION: válida")
    void testTransition_AceptadoToEnPreparacion() {
        testOrder.setEstado(OrderStatus.ACEPTADO);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        OrderResponseDTO updated = orderService.updateStatus(1L, OrderStatus.EN_PREPARACION);

        assertEquals(OrderStatus.EN_PREPARACION, updated.getEstado());
    }

    @Test
    @DisplayName("Transición EN_PREPARACION -> DESPACHADO: válida")
    void testTransition_EnPreparacionToDespachado() {
        testOrder.setEstado(OrderStatus.EN_PREPARACION);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        OrderResponseDTO updated = orderService.updateStatus(1L, OrderStatus.DESPACHADO);

        assertEquals(OrderStatus.DESPACHADO, updated.getEstado());
    }

    @Test
    @DisplayName("Transición DESPACHADO -> ENTREGADO: válida")
    void testTransition_DespachadoToEntregado() {
        testOrder.setEstado(OrderStatus.DESPACHADO);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(i -> i.getArgument(0));

        OrderResponseDTO updated = orderService.updateStatus(1L, OrderStatus.ENTREGADO);

        assertEquals(OrderStatus.ENTREGADO, updated.getEstado());
    }

    @Test
    @DisplayName("Transición no válida (ej. CREADO -> ENTREGADO): debe lanzar InvalidStatusTransitionException")
    void testInvalidTransition_CreadoToEntregado() {
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));

        assertThrows(InvalidStatusTransitionException.class, () ->
                orderService.updateStatus(1L, OrderStatus.ENTREGADO));

        verifyNoInteractions(catalogClient);
        verify(orderRepository, never()).save(any());
    }

    @Test
    @DisplayName("Transición desde estado final (ENTREGADO o CANCELADO): debe lanzar InvalidStatusTransitionException")
    void testTransition_FromFinalState() {
        testOrder.setEstado(OrderStatus.ENTREGADO);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(testOrder));

        assertThrows(InvalidStatusTransitionException.class, () ->
                orderService.updateStatus(1L, OrderStatus.CANCELADO));

        testOrder.setEstado(OrderStatus.CANCELADO);
        assertThrows(InvalidStatusTransitionException.class, () ->
                orderService.updateStatus(1L, OrderStatus.ACEPTADO));
    }

    @Test
    @DisplayName("Buscar pedido inexistente debe lanzar OrderNotFoundException")
    void testFindById_NotFound() {
        when(orderRepository.findById(999L)).thenReturn(Optional.empty());

        assertThrows(OrderNotFoundException.class, () -> orderService.findById(999L));
    }
}
