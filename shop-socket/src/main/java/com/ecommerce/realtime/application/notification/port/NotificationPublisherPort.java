package com.ecommerce.realtime.application.notification.port;

/**
 * Outbound Port cho việc publish notification.
 * KHÔNG có bất kỳ tham chiếu nào đến SimpMessagingTemplate / STOMP / Redis.
 */
public interface NotificationPublisherPort {
    void publish(Object domainEvent);
}