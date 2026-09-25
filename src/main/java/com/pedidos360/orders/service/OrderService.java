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

    public OrderService(OrderRepository orderRepository, CatalogClient catalogClient) {
        this.orderRepository = orderRepository;
        this.catalogClient = catalogClient;
    }

    /**
     * Listar todos los pedidos (para Operador / Admin).
     */
    @Transactional(readOnly = true)
    public List<OrderResponseDTO> findAll() {
        return orderRepository.findAllByOrderByFechaCreacionDesc()
                .stream()
                .map(OrderResponseDTO::fromEntity)
                .toList();
    }

    /**
     * Listar pedidos de un cliente en específico (para Cliente).
     */
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

    /**
     * Buscar un pedido por su identificador único.
     */
    @Transactional(readOnly = true)
    public OrderResponseDTO findById(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
        return OrderResponseDTO.fromEntity(order);
    }

    /**
     * Crear un nuevo pedido con estado inicial CREADO.
     */
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
        return OrderResponseDTO.fromEntity(saved);
    }

    /**
     * Actualizar el estado de un pedido según la máquina de estados.
     * Si la transición es hacia ACEPTADO, descuenta stock de cada item en el catálogo.
     */
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

        // Si la transición es hacia ACEPTADO -> llamar a CatalogClient para descontar stock de cada item
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

        return OrderResponseDTO.fromEntity(updated);
    }
}
