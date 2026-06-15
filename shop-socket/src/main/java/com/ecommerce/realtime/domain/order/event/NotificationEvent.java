package com.ecommerce.realtime.domal.order.event;

public sealed interface NotificationEvent permits OrderStatusChangedEvent, ProductStockChangedEvent {
}
