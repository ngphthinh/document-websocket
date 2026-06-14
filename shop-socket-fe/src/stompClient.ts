/* eslint-disable @typescript-eslint/no-explicit-any */
import { Client } from "@stomp/stompjs";
import SockJS from "sockjs-client";

export const stompClient = new Client({
  webSocketFactory: () => new SockJS("http://localhost:8080/ws"),
  reconnectDelay: 5000,
});

export const service = {
  subscribeAndTrack: (orderId: string, setOrder: any) => {
    const subscription = stompClient.subscribe(
      `/topic/orders/${orderId}`,
      (message) => {
        const body = JSON.parse(message.body);
        setOrder((prev: any) => ({
          ...prev,
          status: body.status,
          message: body.message,
        }));
      },
    );

    stompClient.publish({
      destination: "/app/order/track",
      body: JSON.stringify({ orderId }),
    });

    return subscription;
  },
  subscribeProductViews: (productId: string, setViews: any) => {
    const subscription = stompClient.subscribe(
      `/topic/products/${productId}/views`,
      (message) => {
        const body = JSON.parse(message.body)
        setViews(body)

      } 
    );

    stompClient.publish({
        destination: `/app/product/${productId}/view`,
    })

    return subscription;
  },
};
