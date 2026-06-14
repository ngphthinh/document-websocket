package com.example.realtime.infrastructure.config.messing.websocket.strategy;

import com.example.realtime.infrastructure.config.messing.websocket.event.OrderStatusChangedEvent;
import org.springframework.stereotype.Component;

@Component
public class OrderStatusNotificationUserStrategy implements NotificationStrategy<OrderStatusChangedEvent> {
    @Override
    public Class<OrderStatusChangedEvent> eventType() {
        return OrderStatusChangedEvent.class;
    }

    @Override
    public String destination(OrderStatusChangedEvent event) {
        return "/topic/orders/" + event.userId()+"/notifications";
    }

    @Override
    public Object buildPayload(OrderStatusChangedEvent event) {
        return new OrderStatusNotification(
                event.orderId(),
                event.previousStatus(),
                event.newStatus(),
                event.occurredAt()
        );
    }

    public record OrderStatusNotification(String orderId, String previousStatus, String newStatus, java.time.Instant occurredAt) {
    }
}
