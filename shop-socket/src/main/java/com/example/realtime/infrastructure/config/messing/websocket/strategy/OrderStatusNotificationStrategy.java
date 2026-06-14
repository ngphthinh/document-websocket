package com.example.realtime.infrastructure.config.messing.websocket.strategy;

import com.example.realtime.application.order.OrderService;
import com.example.realtime.infrastructure.config.messing.websocket.event.OrderStatusChangedEvent;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

@Component
public class OrderStatusNotificationStrategy implements NotificationStrategy<OrderStatusChangedEvent> {


    @Override
    public Class<OrderStatusChangedEvent> eventType() {
        return OrderStatusChangedEvent.class;
    }

    @Override
    public String destination(OrderStatusChangedEvent event) {
        return "/topic/orders/" + event.orderId() + "/status";
    }

    @Override
    public Object buildPayload(OrderStatusChangedEvent event) {
        return OrderStatusPayload.from(event);
    }

    public record OrderStatusPayload(String orderId, String status) {
        public static OrderStatusPayload from(OrderStatusChangedEvent e) {
            return new OrderStatusPayload(e.orderId(), e.newStatus());
        }
    }
}
