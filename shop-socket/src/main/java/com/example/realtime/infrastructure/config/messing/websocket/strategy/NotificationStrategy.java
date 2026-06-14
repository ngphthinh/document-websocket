package com.example.realtime.infrastructure.config.messing.websocket.strategy;


import com.example.realtime.infrastructure.config.messing.websocket.event.NotificationEvent;

public interface NotificationStrategy <T  extends NotificationEvent> {
    Class<T> eventType();
    String destination(T event);
    Object buildPayload(T event);
}
