package com.pedidos360.orders.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    @Value("${app.rabbitmq.ordenes.exchange}")
    private String ordenesExchange;

    @Value("${app.rabbitmq.ordenes.queue}")
    private String ordenesQueue;

    @Value("${app.rabbitmq.ordenes.routing-key}")
    private String ordenesRoutingKey;

    @Value("${app.rabbitmq.ordenes.dlx}")
    private String dlxExchange;

    @Value("${app.rabbitmq.ordenes.dlq}")
    private String dlqQueue;

    @Value("${app.rabbitmq.ordenes.dlq-routing-key}")
    private String dlqRoutingKey;

    @Bean
    public DirectExchange ordenesDeadLetterExchange() {
        return new DirectExchange(dlxExchange);
    }

    @Bean
    public Queue ordenesDeadLetterQueue() {
        return QueueBuilder.durable(dlqQueue).build();
    }

    @Bean
    public Binding ordenesDlqBinding() {
        return BindingBuilder.bind(ordenesDeadLetterQueue())
                .to(ordenesDeadLetterExchange())
                .with(dlqRoutingKey);
    }

    @Bean
    public DirectExchange ordenesExchange() {
        return new DirectExchange(ordenesExchange);
    }

    @Bean
    public Queue ordenesQueue() {
        return QueueBuilder.durable(ordenesQueue)
                .withArgument("x-dead-letter-exchange", dlxExchange)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .build();
    }

    @Bean
    public Binding ordenesBinding() {
        return BindingBuilder.bind(ordenesQueue())
                .to(ordenesExchange())
                .with(ordenesRoutingKey);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
