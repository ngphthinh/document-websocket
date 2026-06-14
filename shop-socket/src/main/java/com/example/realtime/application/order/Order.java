package com.example.realtime.application.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@Data
@AllArgsConstructor
public class Order {
    private String orderId;
    private String userId;
    private String status;


}
