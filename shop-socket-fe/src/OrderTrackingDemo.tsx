/* eslint-disable @typescript-eslint/no-explicit-any */
import { useEffect, useState } from "react";
import { websocketService } from "./WebSocketService";
import { orderId } from "./constants";

export default function OrderTrackingDemo() {
  const [order, setOrder] = useState({
    status: "Đang xử lý",
  });
  const { status } = order;
  useEffect(() => {
    let subscription: any;

    (async () => {
      subscription = await websocketService.subscribe(
        `/topic/orders/${orderId}/status`,
        (message) => {
          const body = JSON.parse(message.body);
          setOrder((prev) => ({
            ...prev,
            status: body.status,
          }));
        },
      );

      // await websocketService.publish(`/app/order/track`, { orderId });
    })();

    return () => subscription?.unsubscribe();
  }, []);
  return (
    <div>
      <h3>Đơn hàng #{orderId}</h3>
      <p>Trạng thái: {status}</p>
    </div>
  );
}
