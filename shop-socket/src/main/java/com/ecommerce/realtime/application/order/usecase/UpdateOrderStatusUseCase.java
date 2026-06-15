package com.ecommerce.realtime.application.order.usecase;

import com.ecommerce.realtime.application.notification.port.NotificationPublisherPort;
import com.ecommerce.realtime.application.order.port.OrderRepositoryPort;
import com.ecommerce.realtime.domain.order.model.Order;
import com.ecommerce.realtime.domain.order.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class UpdateOrderStatusUseCase {

    private final OrderRepositoryPort orderRepository;
    private final NotificationPublisherPort notificationPublisher;

    @Transactional
    public void execute(String orderId, OrderStatus newStatus) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        order.changeStatus(newStatus); // Business rule trong Domain
        orderRepository.save(order);

        // Drain & publish domain events - Application Layer điều phối,
        // nhưng không biết chi tiết "publish" nghĩa là gì (WebSocket? Redis? Cả hai?)
        order.pullDomainEvents().forEach(notificationPublisher::publish);
    }
}