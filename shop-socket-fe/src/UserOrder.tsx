/* eslint-disable @typescript-eslint/no-explicit-any */
import { useEffect, useState } from "react";
import { websocketService } from "./WebSocketService";
import { orderId } from "./constants";

interface UserOrderProps {
  userId: string;
}

export default function UserOrder({ userId }: UserOrderProps) {
  const [message, setMessage] = useState("Chua co");
  useEffect(() => {
    let subscription: any;

    (async () => {
      subscription = await websocketService.subscribe(
        `/topic/users/${userId}/notifications`,
        (message) => {
          setMessage(message.body);
        },
      );

      // await websocketService.publish(`/app/order/track`, { orderId });
    })();

    return () => subscription?.unsubscribe();
  }, [userId]);
  return (
    <div>
      <h3>Đơn hàng #{userId}</h3>
      <p>{message}</p>
    </div>
  );
}
