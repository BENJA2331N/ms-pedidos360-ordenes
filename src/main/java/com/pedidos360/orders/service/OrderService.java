package com.pedidos360.orders.service;

import com.pedidos360.orders.dto.OrderItemRequestDTO;
import com.pedidos360.orders.dto.OrderNotificationEventDTO;
import com.pedidos360.orders.dto.OrderRequestDTO;
import com.pedidos360.orders.dto.OrderResponseDTO;
import com.pedidos360.orders.dto.OrderTicketEventDTO;
import com.pedidos360.orders.exception.InvalidStatusTransitionException;
import com.pedidos360.orders.exception.OrderNotFoundException;
import com.pedidos360.orders.messaging.producer.OrderEventProducer;
import com.pedidos360.orders.model.Order;
import com.pedidos360.orders.model.OrderItem;
import com.pedidos360.orders.model.OrderStatus;
import com.pedidos360.orders.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Transactional
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private static final Map<OrderStatus, Set<OrderStatus>> TRANSICIONES_VALIDAS = Map.of(
            OrderStatus.CREADO, Set.of(OrderStatus.ACEPTADO, OrderStatus.CANCELADO),
            OrderStatus.ACEPTADO, Set.of(OrderStatus.EN_PREPARACION, OrderStatus.CANCELADO),
            OrderStatus.EN_PREPARACION, Set.of(OrderStatus.DESPACHADO, OrderStatus.CANCELADO),
            OrderStatus.DESPACHADO, Set.of(OrderStatus.ENTREGADO),
            OrderStatus.ENTREGADO, Set.of(),
            OrderStatus.CANCELADO, Set.of()
    );

    private final OrderRepository orderRepository;
    private final CatalogClient catalogClient;
    private final OrderEventProducer orderEventProducer;

    public OrderService(OrderRepository orderRepository, CatalogClient catalogClient,
                        OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.catalogClient = catalogClient;
        this.orderEventProducer = orderEventProducer;
    }

    @Transactional(readOnly = true)
    public List<OrderResponseDTO> findAll() {
        return orderRepository.findAllByOrderByFechaCreacionDesc()
                .stream()
                .map(OrderResponseDTO::fromEntity)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<OrderResponseDTO> findByCliente(String clienteId) {
        if (clienteId == null || clienteId.isBlank()) {
            throw new IllegalArgumentException("El clienteId no puede estar vacío");
        }
        return orderRepository.findByClienteIdOrderByFechaCreacionDesc(clienteId)
                .stream()
                .map(OrderResponseDTO::fromEntity)
                .toList();
    }

    @Transactional(readOnly = true)
    public OrderResponseDTO findById(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
        return OrderResponseDTO.fromEntity(order);
    }

    public OrderResponseDTO create(OrderRequestDTO dto, String clienteId) {
        if (clienteId == null || clienteId.isBlank()) {
            throw new IllegalArgumentException("El identificador del cliente (clienteId) es obligatorio para crear el pedido.");
        }
        if (dto.getItems() == null || dto.getItems().isEmpty()) {
            throw new IllegalArgumentException("El pedido debe contener al menos un producto.");
        }

        Order order = new Order();
        order.setClienteId(clienteId);
        order.setEstado(OrderStatus.CREADO);
        order.setFechaCreacion(LocalDateTime.now());

        for (OrderItemRequestDTO itemDto : dto.getItems()) {
            OrderItem item = new OrderItem(
                    itemDto.getProductoId(),
                    itemDto.getCantidad(),
                    itemDto.getPrecioUnitario()
            );
            order.addItem(item);
        }

        order.calcularTotal();

        Order saved = orderRepository.save(order);
        log.info("Pedido creado exitosamente con ID: {} para cliente: {}, total: {}",
                saved.getId(), clienteId, saved.getTotal());

        // Tareas asíncronas desacopladas por dominio: 1) Notificación al cliente, 2) Generación de ticket
        orderEventProducer.publicarNotificacionOrden(new OrderNotificationEventDTO(
                saved.getId(),
                saved.getEstado().name(),
                "Notificación de nueva orden creada para cliente " + clienteId
        ));
        orderEventProducer.publicarTicketOrden(new OrderTicketEventDTO(
                saved.getId(),
                clienteId,
                saved.getEstado().name(),
                saved.getTotal(),
                saved.getItems().size(),
                "Comprobante de recepción de pedido #" + saved.getId()
        ));

        return OrderResponseDTO.fromEntity(saved);
    }

    public OrderResponseDTO updateStatus(Long id, OrderStatus nuevoEstado) {
        if (nuevoEstado == null) {
            throw new IllegalArgumentException("El nuevo estado no puede ser nulo.");
        }

        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));

        OrderStatus estadoActual = order.getEstado();
        Set<OrderStatus> permitidas = TRANSICIONES_VALIDAS.getOrDefault(estadoActual, Set.of());

        if (!permitidas.contains(nuevoEstado)) {
            log.warn("Transición rechazada para pedido ID {}: intento de cambiar de {} a {}", id, estadoActual, nuevoEstado);
            throw new InvalidStatusTransitionException(estadoActual, nuevoEstado);
        }

        if (nuevoEstado == OrderStatus.ACEPTADO) {
            log.info("Transición hacia ACEPTADO para pedido ID {}. Descontando stock en catálogo para {} items...",
                    id, order.getItems().size());
            for (OrderItem item : order.getItems()) {
                catalogClient.descontarStock(item.getProductoId(), item.getCantidad());
            }
        }

        order.setEstado(nuevoEstado);
        Order updated = orderRepository.save(order);
        log.info("Estado del pedido ID {} actualizado a: {}", id, nuevoEstado);

        orderEventProducer.publicarNotificacionOrden(new OrderNotificationEventDTO(
                updated.getId(),
                updated.getEstado().name(),
                "Notificación de cambio de estado de la orden a " + nuevoEstado.name()
        ));

        if (nuevoEstado == OrderStatus.ACEPTADO || nuevoEstado == OrderStatus.ENTREGADO) {
            orderEventProducer.publicarTicketOrden(new OrderTicketEventDTO(
                    updated.getId(),
                    updated.getClienteId(),
                    updated.getEstado().name(),
                    updated.getTotal(),
                    updated.getItems().size(),
                    "Documento electrónico de estado " + nuevoEstado.name() + " para pedido #" + updated.getId()
            ));
        }

        return OrderResponseDTO.fromEntity(updated);
    }
}
