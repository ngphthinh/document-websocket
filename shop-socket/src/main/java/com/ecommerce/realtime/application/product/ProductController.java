package com.ecommerce.realtime.application.product;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/products")
@CrossOrigin(origins = "*")
public class ProductController {

    public final ProductService productService;

    @PatchMapping("/{productId}/stock")
    public void changeStock(@PathVariable String productId, @RequestBody ChangeStockRequest request) {
        productService.changeStock(productId, request.stock);
    }

    public record ChangeStockRequest(int stock) { }

}
