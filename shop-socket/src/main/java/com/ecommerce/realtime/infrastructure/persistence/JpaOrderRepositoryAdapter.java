package com.ecommerce.realtime.infrastructure.persistence;

import com.ecommerce.realtime.application.order.port.OrderRepositoryPort;
import com.ecommerce.realtime.domain.order.model.Order;
import com.ecommerce.realtime.domain.order.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class JpaOrderRepositoryAdapter implements OrderRepositoryPort {

    private final OrderJpaRepository jpaRepository; // Spring Data JPA interface

    @Override
    public Optional<Order> findById(String orderId) {
        return jpaRepository.findById(orderId)
                .map(entity -> new Order(entity.getId(), entity.getUserId(),
                        OrderStatus.valueOf(entity.getStatus())));
    }

    @Override
    public void save(Order order) {
        OrderJpaEntity entity = jpaRepository.findById(order.getId())
                .orElse(new OrderJpaEntity(order.getId(), order.getUserId(), order.getStatus().name()));
        entity.setStatus(order.getStatus().name());
        jpaRepository.save(entity);
    }
}