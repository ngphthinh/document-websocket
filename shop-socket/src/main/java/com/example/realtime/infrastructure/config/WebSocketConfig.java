package com.example.realtime.infrastructure.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.web.socket.config.annotation.*;

@Slf4j
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * Đăng ký endpoint mà client sẽ kết nối đến để bắt đầu handshake WebSocket.
     * SockJS được bật để hỗ trợ fallback cho các browser/môi trường không hỗ trợ WebSocket trực tiếp.
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*") // Production: chỉ định domain cụ thể (Chương 17)
                .withSockJS();                 // Fallback HTTP streaming/polling nếu WebSocket bị chặn
    }

    /**
     * Cấu hình Message Broker:
     * - enableSimpleBroker: dùng broker in-memory cho các destination /topic, /queue
     * - setApplicationDestinationPrefixes: message client gửi lên có prefix /app
     *   sẽ được route đến các @MessageMapping
     * - setUserDestinationPrefix: dùng cho gửi message đến user cụ thể (Chương 7)
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic", "/queue")
                .setHeartbeatValue(new long[]{10000, 10000}) // heartbeat 10s (Chương 18)
                .setTaskScheduler(heartBeatScheduler());

        registry.setApplicationDestinationPrefixes("/app");
        registry.setUserDestinationPrefix("/user");
    }

    @org.springframework.context.annotation.Bean
    public org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler heartBeatScheduler() {
        org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler scheduler =
                new org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("ws-heartbeat-");
        scheduler.initialize();
        return scheduler;
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(new ChannelInterceptor() {
            @Override
            public Message<?> preSend(Message<?> message, MessageChannel channel) {
                StompHeaderAccessor accessor = StompHeaderAccessor.wrap(message);
                log.debug("STOMP IN: command={}, destination={}, sessionId={}",
                        accessor.getCommand(), accessor.getDestination(), accessor.getSessionId());
                return message;
            }
        });
        registration.taskExecutor()
                .corePoolSize(4) // Tùy chỉnh số lượng thread pool cho xử lý message inbound (client gửi lên)
                .maxPoolSize(10)// Tối đa 10 thread
                .queueCapacity(100); // Queue tối đa 100 message chờ xử lý (tùy chỉnh theo nhu cầu)
    }
}