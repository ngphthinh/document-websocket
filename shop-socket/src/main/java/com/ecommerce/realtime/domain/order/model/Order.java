package com.ecommerce.realtime.domain.order.model;

import com.ecommerce.realtime.domain.order.event.OrderStatusChangedEvent;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregate Root - chứa business logic thuần, không phụ thuộc Spring/JPA/WebSocket.
 */
@Getter
public class Order {

    private final String id;
    private final String userId;
    private OrderStatus status;

    // Domain Events được tích lũy, sẽ được "drain" bởi Application Layer sau khi save
    private final List<Object> domainEvents = new ArrayList<>();

    public Order(String id, String userId, OrderStatus status) {
        this.id = id;
        this.userId = userId;
        this.status = status;
    }

    /**
     * Business rule: chỉ cho phép chuyển trạng thái theo đúng quy trình.
     */
    public void changeStatus(OrderStatus newStatus) {
        if (!status.canTransitionTo(newStatus)) {
            throw new IllegalStateException(
                    "Không thể chuyển trạng thái từ " + status + " sang " + newStatus);
        }
        OrderStatus previous = this.status;
        this.status = newStatus;
        this.domainEvents.add(OrderStatusChangedEvent.of(id, userId, previous.name(), newStatus.name()));
    }

    public List<Object> pullDomainEvents() {
        List<Object> events = new ArrayList<>(domainEvents);
        domainEvents.clear();
        return events;
    }
}