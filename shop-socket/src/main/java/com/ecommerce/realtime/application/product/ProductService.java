package com.ecommerce.realtime.application.product;

import com.ecommerce.realtime.domain.order.event.ProductStockChangedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProductService {


    private final ApplicationEventPublisher eventPublisher;

    public void changeStock(String productId, int stock) {
        ProductStockChangedEvent event = ProductStockChangedEvent.builder()
                .productId(productId)
                .stock(stock)
                .build();

        eventPublisher.publishEvent(event);
    }
}
