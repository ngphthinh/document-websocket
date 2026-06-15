package com.ecommerce.realtime.domain.order.event;

import lombok.Builder;

@Builder
public record ProductStockChangedEvent(
        String productId,
        int stock
) implements NotificationEvent {
}
