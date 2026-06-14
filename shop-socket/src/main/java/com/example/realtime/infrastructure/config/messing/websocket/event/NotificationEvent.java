package com.example.realtime.infrastructure.config.messing.websocket.event;

public sealed interface NotificationEvent permits OrderStatusChangedEvent, ProductStockChangedEvent {
}
