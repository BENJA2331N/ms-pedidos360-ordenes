package com.pedidos360.orders.repository;

import com.pedidos360.orders.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    List<Order> findByClienteId(String clienteId);

    List<Order> findByClienteIdOrderByFechaCreacionDesc(String clienteId);

    List<Order> findAllByOrderByFechaCreacionDesc();
}
