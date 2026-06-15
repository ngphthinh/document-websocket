package com.ecommerce.realtime.application.order.port;

import com.ecommerce.realtime.domain.order.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * Outbound Port - Application Layer định nghĩa "cần gì",
 * Infrastructure Layer sẽ implement "làm như thế nào" (JPA, in-memory, v.v.)
 */
public interface OrderRepositoryPort {
    Optional<Order> findById(String orderId);
    void save(Order order);
}