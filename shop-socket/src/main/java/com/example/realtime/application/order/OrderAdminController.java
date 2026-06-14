package com.example.realtime.application.order;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class OrderAdminController {

    private final OrderService orderService;

    @PutMapping("/{orderId}/status")
    public void updateStatus(@PathVariable String orderId, @RequestBody UpdateStatusRequest request) {
        orderService.updateOrderStatus(orderId, request.status());
    }

    public record UpdateStatusRequest(String status) {}
}