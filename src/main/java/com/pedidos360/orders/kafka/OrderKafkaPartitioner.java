package com.pedidos360.orders.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Particionador clásico para demostrar de forma determinista el comportamiento de Kafka:
 * - Sin Key (key == null): distribución Round-Robin estricta entre las particiones (0, 1, 2...).
 * - Con Key (key != null): hash Murmur2 sobre la clave para enviar siempre a la misma partición
 *   y garantizar el orden estricto de los eventos de un mismo pedido.
 */
public class OrderKafkaPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null || keyBytes.length == 0) {
            return Math.floorMod(counter.getAndIncrement(), numPartitions);
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
        // Sin recursos externos que liberar
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Sin configuración adicional requerida
    }
}
