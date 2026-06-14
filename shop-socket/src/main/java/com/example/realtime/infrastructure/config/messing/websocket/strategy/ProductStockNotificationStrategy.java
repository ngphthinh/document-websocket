package com.example.realtime.infrastructure.config.messing.websocket.strategy;

import com.example.realtime.infrastructure.config.messing.websocket.event.ProductStockChangedEvent;
import org.springframework.stereotype.Component;

@Component
public class ProductStockNotificationStrategy implements NotificationStrategy<ProductStockChangedEvent> {

    @Override
    public Class<ProductStockChangedEvent> eventType() {
        return ProductStockChangedEvent.class;
    }

    @Override
    public String destination(ProductStockChangedEvent event) {
        return "/topic/products/" + event.productId() + "/stock";
    }

    @Override
    public Object buildPayload(ProductStockChangedEvent event) {
        return ProductStockPayload.from(event);
    }

    public record ProductStockPayload(String productId, int stock) {
        public static ProductStockPayload from(ProductStockChangedEvent e) {
            return new ProductStockPayload(e.productId(), e.stock());
        }
    }
}

