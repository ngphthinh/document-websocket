package com.example.realtime.application.order;


import com.example.realtime.infrastructure.config.messing.websocket.event.OrderStatusChangedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;


@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final ApplicationEventPublisher eventPublisher;

    public void updateOrderStatus(String orderId, String newStatus) {
        // mockdata:
        Order order =  Order.builder()
                .status("SHIPPING")
                .orderId(orderId)
                .userId("user-123")
                .build();

        String previousStatus = order.getStatus();
        order.setStatus(newStatus);
        log.info("Order {} status updated: {} -> {}", orderId, previousStatus, newStatus);
        //TODO: SAVE order vào database (Repository)
        // Publish domain event - decoupled khỏi WebSocket layer (Producer)
        eventPublisher.publishEvent(
                OrderStatusChangedEvent.of(orderId, order.getUserId(), previousStatus, newStatus)
        );

    }


}
