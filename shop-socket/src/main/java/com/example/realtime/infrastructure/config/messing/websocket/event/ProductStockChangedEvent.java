package com.example.realtime.infrastructure.config.messing.websocket.event;

import lombok.Builder;

@Builder
public record ProductStockChangedEvent(
        String productId,
        int stock
) implements NotificationEvent {
}
