import React, { useEffect } from "react";
import { websocketService } from "./WebSocketService";

interface ProductViewsProps {
  productId: string;
}

const ProductViews: React.FC<ProductViewsProps> = ({ productId }) => {
  const [message, setMessage] = React.useState("");

  useEffect(() => {
    let subscription: any;

    (async () => {
      subscription = await websocketService.subscribe(
        `/topic/products/${productId}/stock`,
        (message) => {
          console.log(message.body);
          setMessage(message.body);
        },
      );

      // await websocketService.publish(`/app/product/${productId}/view`);
    })();

    return () => {
      subscription?.unsubscribe();
    };
  }, [productId]);

  return (
    <div>
      <h3>
        tồn kho {productId}: {message}
      </h3> 
    </div>
  );
};

export default ProductViews;
