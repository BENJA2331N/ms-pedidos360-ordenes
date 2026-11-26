package com.pedidos360.orders.kafka.domain;

import com.pedidos360.orders.kafka.dto.InventoryStreamingEventDTO;
import com.pedidos360.orders.kafka.dto.OrderStreamingEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Servicio de dominio desacoplado de la infraestructura de mensajería (@KafkaListener).
 * Procesa la analítica en tiempo real de órdenes, el registro de auditoría y la
 * sincronización del estado de inventario proveniente de ms-pedidos360-catalogo.
 */
@Service
public class OrderStreamingDomainService {

    private static final Logger log = LoggerFactory.getLogger(OrderStreamingDomainService.class);

    private final AtomicLong totalEventosAnalitica = new AtomicLong(0);
    private final AtomicReference<BigDecimal> montoAcumuladoCreados = new AtomicReference<>(BigDecimal.ZERO);
    private final Map<String, AtomicLong> conteoPorEstado = new ConcurrentHashMap<>();
    private final Deque<String> bitacoraAuditoria = new ConcurrentLinkedDeque<>();
    private final Map<Long, Integer> stockConocidoPorProducto = new ConcurrentHashMap<>();

    /**
     * Lógica de dominio para el Consumer Group de Analítica (pedidos360-ordenes-analitica-group).
     */
    public void procesarAnaliticaOrden(OrderStreamingEventDTO evento) {
        if ("SIMULAR_FALLO_TRANSITORIO".equalsIgnoreCase(evento.getOrigen())) {
            throw new RuntimeException("Fallo transitorio simulado en motor de analítica de órdenes");
        }

        totalEventosAnalitica.incrementAndGet();
        conteoPorEstado.computeIfAbsent(evento.getEstado(), k -> new AtomicLong(0)).incrementAndGet();
        if ("CREADO".equalsIgnoreCase(evento.getEstado()) && evento.getTotal() != null) {
            montoAcumuladoCreados.updateAndGet(actual -> actual.add(evento.getTotal()));
        }

        log.info("[DOMINIO-ANALITICA] Orden #{} procesada | estado={} total={} acumuladoEventos={}",
                evento.getOrderId(), evento.getEstado(), evento.getTotal(), totalEventosAnalitica.get());
    }

    /**
     * Lógica de dominio para el Consumer Group de Auditoría (pedidos360-ordenes-auditoria-group).
     */
    public void registrarAuditoriaOrden(OrderStreamingEventDTO evento) {
        String entrada = String.format("[%s] eventId=%s orderId=%d cliente=%s estado=%s total=%s",
                evento.getTimestamp(), evento.getEventId(), evento.getOrderId(),
                evento.getClienteId(), evento.getEstado(), evento.getTotal());
        bitacoraAuditoria.addFirst(entrada);
        while (bitacoraAuditoria.size() > 50) {
            bitacoraAuditoria.removeLast();
        }
        log.info("[DOMINIO-AUDITORIA] Evento auditado exitosamente: {}", entrada);
    }

    /**
     * Lógica de dominio para la lectura del segundo tópico (pedidos360.inventario.eventos).
     */
    public void sincronizarProyeccionInventario(InventoryStreamingEventDTO evento) {
        if ("SIMULAR_FALLO_TRANSITORIO".equalsIgnoreCase(evento.getOrigen())) {
            throw new RuntimeException("Fallo transitorio simulado en proyección de inventario");
        }
        stockConocidoPorProducto.put(evento.getProductId(), evento.getStockRestante());
        log.info("[DOMINIO-INVENTARIO-SYNC] Proyección local actualizada | productoId={} ('{}') mov={} stockRestante={}",
                evento.getProductId(), evento.getProductoNombre(), evento.getTipoMovimiento(), evento.getStockRestante());
    }

    public Map<String, Object> obtenerResumenMetricas() {
        Map<String, Long> porEstado = new LinkedHashMap<>();
        conteoPorEstado.forEach((k, v) -> porEstado.put(k, v.get()));

        Map<String, Object> resumen = new LinkedHashMap<>();
        resumen.put("totalEventosAnalitica", totalEventosAnalitica.get());
        resumen.put("montoAcumuladoPedidosCreados", montoAcumuladoCreados.get());
        resumen.put("eventosPorEstado", porEstado);
        resumen.put("stockSincronizadoPorProducto", Map.copyOf(stockConocidoPorProducto));
        resumen.put("ultimasAuditorias", List.copyOf(bitacoraAuditoria));
        return resumen;
    }
}
