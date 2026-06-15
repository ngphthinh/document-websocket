package com.ecommerce.realtime.domal.order.event;

import lombok.Builder;

@Builder
public record ProductStockChangedEvent(
        String productId,
        int stock
) implements NotificationEvent {
}
