package com.example.realtime.infrastructure.config.messing.websocket;

import com.example.realtime.infrastructure.config.messing.websocket.event.NotificationEvent;
import com.example.realtime.infrastructure.config.messing.websocket.strategy.NotificationStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.List;


@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationPublisher {
    private final SimpMessagingTemplate messagingTemplate;

    private final List<
            NotificationStrategy<? extends NotificationEvent>
            > strategies;
    // Strategy: Listen to onNotificationEvent and publish to WebSocket topic
    // nên thêm transactionalEventListener để đảm bảo chỉ publish khi transaction thành công
    // @Async : để không block luồng chính khi publish, nhưng cần đảm bảo thread safety nếu có shared state
    @EventListener
    @SuppressWarnings("unchecked")
    public void onNotificationEvent(NotificationEvent event) {

        strategies.stream()
                .filter(s -> s.eventType().equals(event.getClass()))
                .forEach(strategy -> {

                    NotificationStrategy<NotificationEvent> s =
                            (NotificationStrategy<NotificationEvent>) strategy;

                    String destination = s.destination(event);
                    Object payload = s.buildPayload(event);

                    messagingTemplate.convertAndSend(destination, payload);
                });
    }

}
