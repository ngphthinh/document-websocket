package com.ecommerce.realtime.domain.order.event;

public sealed interface NotificationEvent permits OrderStatusChangedEvent, ProductStockChangedEvent {
}
