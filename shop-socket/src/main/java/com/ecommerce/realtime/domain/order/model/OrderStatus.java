package com.ecommerce.realtime.domain.order.model;

import java.util.Set;

public enum OrderStatus {
    PENDING, CONFIRMED, PACKED, SHIPPING, DELIVERED, CANCELLED;

    private static final java.util.Map<OrderStatus, Set<OrderStatus>> TRANSITIONS = java.util.Map.of(
            PENDING,   Set.of(CONFIRMED, CANCELLED),
            CONFIRMED, Set.of(PACKED, CANCELLED),
            PACKED,    Set.of(SHIPPING, CANCELLED),
            SHIPPING,  Set.of(DELIVERED),
            DELIVERED, Set.of(),
            CANCELLED, Set.of()
    );

    public boolean canTransitionTo(OrderStatus target) {
        return TRANSITIONS.get(this).contains(target);
    }
}