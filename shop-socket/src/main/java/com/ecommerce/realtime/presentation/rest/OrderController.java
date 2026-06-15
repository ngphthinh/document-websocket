package com.ecommerce.realtime.presentation.rest;

import com.ecommerce.realtime.application.order.usecase.UpdateOrderStatusUseCase;
import com.ecommerce.realtime.domain.order.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final UpdateOrderStatusUseCase updateOrderStatusUseCase;

    @PutMapping("/{orderId}/status")
    public void updateStatus(@PathVariable String orderId, @RequestBody UpdateStatusRequest request) {
        updateOrderStatusUseCase.execute(orderId, OrderStatus.valueOf(request.status()));
    }

    public record UpdateStatusRequest(String status) {}
}