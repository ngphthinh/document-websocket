# KHÓA HỌC WEBSOCKET CHUYÊN SÂU CHO JAVA SPRING BOOT DEVELOPER
## Từ Fresher đến Senior - Xây dựng Hệ thống Ecommerce Realtime

> **Tech stack:** Java 21 · Spring Boot 3.x · Maven · Lombok · Spring WebSocket · STOMP · Spring Security · Redis · PostgreSQL · Docker · React (client demo)

---

## 1. GIỚI THIỆU KHÓA HỌC

Khóa học này được thiết kế để đưa bạn từ một Java Developer mới tìm hiểu về Realtime Communication
đến một **Senior Backend Engineer** có khả năng thiết kế, triển khai và vận hành một hệ thống
WebSocket ở quy mô production cho nền tảng **Ecommerce**.

Toàn bộ khóa học xoay quanh **một bài toán nghiệp vụ duy nhất**: hệ thống **Ecommerce Realtime
Platform**, bao gồm:

- Theo dõi trạng thái đơn hàng theo thời gian thực (Realtime Order Tracking)
- Hệ thống thông báo cá nhân hóa (Personalized Notification Center)
- Dashboard quản trị theo thời gian thực (Realtime Admin Dashboard)
- Theo dõi người dùng online (Online User Tracking)

Mỗi chương sẽ **xây dựng thêm một phần** của hệ thống này, theo đúng **Clean Architecture**,
**SOLID**, **DDD** và **Hexagonal Architecture**, để cuối khóa bạn có một codebase hoàn chỉnh,
có thể dùng làm **portfolio** hoặc **template production**.

---

## 2. CẤU TRÚC PACKAGE CHUẨN (ÁP DỤNG XUYÊN SUỐT KHÓA HỌC)

```
src/main/java/com/ecommerce/realtime
├── domain
│   ├── order
│   │   ├── model            // Entity, Value Object, Aggregate Root
│   │   ├── event             // Domain Events
│   │   └── exception
│   ├── notification
│   └── user
├── application
│   ├── order
│   │   ├── usecase           // Use Case / Application Service
│   │   └── port              // Inbound/Outbound Port (interfaces)
│   └── notification
├── infrastructure
│   ├── persistence           // JPA Repository, Entity Mapper
│   ├── messaging
│   │   ├── websocket          // STOMP Config, Adapter
│   │   └── redis              // Redis Pub/Sub
│   ├── security                // JWT, Handshake Interceptor
│   └── config
├── presentation
│   ├── rest                   // REST Controller
│   └── websocket               // WebSocket/STOMP Controller
└── shared
    ├── kernel                  // Base classes (AggregateRoot, ValueObject...)
    └── util
```

**Nguyên tắc cốt lõi:**

| Layer | Vai trò | Không phụ thuộc vào |
|---|---|---|
| `domain` | Business logic thuần, không phụ thuộc framework | Spring, JPA, WebSocket |
| `application` | Điều phối use case, định nghĩa Port | Infrastructure cụ thể |
| `infrastructure` | Triển khai chi tiết kỹ thuật (DB, Redis, WebSocket) | - |
| `presentation` | Tầng giao tiếp (REST/WebSocket Controller) | - |

---

## 3. ROADMAP HỌC TẬP

### 🟢 GIAI ĐOẠN 1 — NỀN TẢNG (Fresher → Junior)
| Chương | Nội dung | Output |
|---|---|---|
| 1 | Tổng quan Realtime Communication | Hiểu Polling, SSE, WebSocket |
| 2 | WebSocket Protocol Fundamentals | Hiểu handshake, frame, lifecycle |
| 3 | Spring Boot WebSocket Setup | Project khởi tạo + Endpoint đầu tiên |
| 4 | STOMP Protocol | Thiết kế topic cho Ecommerce |
| 5 | Producer/Consumer Pattern | Notification Order Service đầu tiên |

### 🟡 GIAI ĐOẠN 2 — TRUNG CẤP (Junior → Middle)
| Chương | Nội dung | Output |
|---|---|---|
| 6 | Clean Architecture cho WebSocket | Refactor theo Hexagonal |
| 7 | User-specific Messaging | Thông báo cá nhân hóa |
| 8 | Authentication & Authorization | JWT Handshake Security |
| 9 | Session Management | Online User Registry |
| 10 | Notification System | Notification Center hoàn chỉnh |

### 🔴 GIAI ĐOẠN 3 — NÂNG CAO (Middle → Senior)
| Chương | Nội dung | Output |
|---|---|---|
| 11 | Scaling WebSocket Systems | Multi-instance, Sticky Session |
| 12 | Redis Pub/Sub Integration | Distributed Broadcast |
| 13 | Realtime Order Tracking | Trang tracking hoàn chỉnh |
| 14 | Realtime Dashboard | Admin Dashboard Realtime |
| 15 | Production Ecommerce Realtime Project | Hệ thống microservices hoàn chỉnh |

### ⚫ GIAI ĐOẠN 4 — CHUYÊN SÂU (Senior)
| Chương | Nội dung |
|---|---|
| 16 | WebSocket Performance Tuning |
| 17 | WebSocket Security |
| 18 | WebSocket Best Practices |
| 19 | So sánh WebSocket vs SSE / Kafka / RabbitMQ / Polling |
| 20 | WebSocket + Kafka Integration |

---

## 4. CÁCH SỬ DỤNG KHÓA HỌC

1. Học theo đúng thứ tự chương — mỗi chương phụ thuộc vào code của chương trước.
2. Làm đầy đủ **Hands-on Exercises** trước khi xem **Reference Answers**.
3. Với **Advanced Exercises**, hãy tự thiết kế trước khi đối chiếu gợi ý.
4. Dùng **Interview Questions** cuối mỗi chương để tự kiểm tra trước khi chuyển chương.
5. Hoàn thành **Completion Checklist** để đảm bảo không bỏ sót kiến thức nền tảng.

---

## 5. DANH SÁCH FILE TRONG KHÓA HỌC

00-README.md                          - File này
# Mục lục

- [Chương 1 - Realtime Communication](./chap01.md)
- [Chương 2 - WebSocket Fundamentals](./chap02.md)
- [Chương 3 - Spring Boot WebSocket Setup](./chap03.md)
- [Chương 4 - STOMP Protocol](./chap04.md)
- [Chương 5 - Producer Consumer Pattern](./chap05.md)
- [Chương 6 - Clean Architecture](./chap06.md)
- [Chương 7 - User Specific Messaging](./chap07.md)
- [Chương 8 - Authentication & Authorization](./chap08.md)
- [Chương 9 - Session Management](./chap09.md)
- [Chương 10 - Notification System](./chap10.md)
- [Chương 11 - Scaling WebSocket](./chap11.md)
- [Chương 12 - Redis Pub/Sub Integration](./chap12.md)
- [Chương 13 - Realtime Order Tracking](./chap13.md)
- [Chương 14 - Realtime Dashboard](./chap14.md)
- [Chương 15 - Production Project (Capstone)](./chap15.md)

## Nâng cao

- [Chương 16 - Performance Tuning](./chap16.md)
- [Chương 17 - WebSocket Security](./chap17.md)
- [Chương 18 - Best Practices](./chap18.md)
- [Chương 19 - Technology Comparisons](./chap19.md)
- [Chương 20 - Kafka Integration](./chap20.md)
```

Chúc bạn học tập hiệu quả và sớm trở thành **Senior Realtime Backend Engineer**! 🚀