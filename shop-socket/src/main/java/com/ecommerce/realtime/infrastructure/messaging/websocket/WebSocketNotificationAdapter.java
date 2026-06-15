package com.ecommerce.realtime.infrastructure.messaging.websocket;

import com.ecommerce.realtime.application.notification.port.NotificationPublisherPort;
import com.ecommerce.realtime.domain.order.event.OrderStatusChangedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Adapter - implement NotificationPublisherPort bằng STOMP/WebSocket.
 * Đây là NƠI DUY NHẤT trong toàn bộ hệ thống "biết" về SimpMessagingTemplate.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketNotificationAdapter implements NotificationPublisherPort {

    private final SimpMessagingTemplate messagingTemplate;

    @Override
    public void publish(Object domainEvent) {
        if (domainEvent instanceof OrderStatusChangedEvent event) {
            publishOrderStatusChanged(event);
        }
        // Khi có thêm domain event khác (ProductStockChangedEvent...),
        // thêm "else if" hoặc refactor sang Strategy Pattern (Chương 5 - Bài 4)
    }

    private void publishOrderStatusChanged(OrderStatusChangedEvent event) {
        log.info("WS publish: orderId={}, status={}", event.orderId(), event.newStatus());

        messagingTemplate.convertAndSend(
                "/topic/orders/" + event.orderId(),
                new OrderStatusMessage(event.orderId(), event.newStatus(), event.occurredAt().toString())
        );
    }

    public record OrderStatusMessage(String orderId, String status, String occurredAt) {}
}