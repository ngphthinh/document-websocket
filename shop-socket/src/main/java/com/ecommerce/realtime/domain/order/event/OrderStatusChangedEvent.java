package com.ecommerce.realtime.domain.order.event;

import java.time.Instant;

public record OrderStatusChangedEvent (
            String orderId,
            String userId,
            String previousStatus,
            String newStatus,
            Instant occurredAt
    )   implements NotificationEvent {
        public static OrderStatusChangedEvent of(String orderId, String userId, String prev, String next) {
            return new OrderStatusChangedEvent(orderId, userId, prev, next, Instant.now());
        }
    }