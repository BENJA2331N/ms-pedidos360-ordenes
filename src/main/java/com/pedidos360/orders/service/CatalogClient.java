package com.pedidos360.orders.service;

import com.pedidos360.orders.dto.StockUpdateDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
public class CatalogClient {

    private static final Logger log = LoggerFactory.getLogger(CatalogClient.class);

    private final RestClient restClient;
    private final String catalogBaseUrl;

    public CatalogClient(
            RestClient.Builder restClientBuilder,
            @Value("${catalog.service.url:http://localhost:8081}") String catalogBaseUrl) {
        this.catalogBaseUrl = catalogBaseUrl;
        this.restClient = restClientBuilder.requestFactory(new JdkClientHttpRequestFactory()).build();
    }

    public void descontarStock(Long productoId, int cantidad) {
        log.info("Llamando a microservicio catalog para descontar stock. Producto ID: {}, Cantidad: {}", productoId, cantidad);
        restClient.patch()
                .uri(catalogBaseUrl + "/api/catalog/products/{id}/stock", productoId)
                .body(new StockUpdateDTO(cantidad))
                .retrieve()
                .toBodilessEntity();
        log.info("Stock descontado exitosamente en catálogo para producto ID: {}", productoId);
    }
}
