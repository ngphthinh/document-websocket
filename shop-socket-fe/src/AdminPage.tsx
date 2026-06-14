import { orderId } from "./constants";
import axios from "axios";
import OrderTrackingDemo from "./OrderTrackingDemo";
import UserOrder from "./UserOrder";
import ProductViews from "./ProductViews";

const AdminPage = () => {
  const handleUpdate = async (orderId: string) => {
    try {
      await axios.put(`http://localhost:8080/api/orders/${orderId}/status`, {
        status: "NEW_STATUS",
      });
    } catch (e: any) {
      throw new Error(e.message);
    }
  };

  const handleUpdateStock = async (productId: string) => {
    try {
      await axios.patch(`http://localhost:8080/api/products/${productId}/stock`, {
        stock: Math.floor(Math.random() * 100),
      });
    } catch (e: any) {
      throw new Error(e.message);
    }
  };

  return (
    <div>
      <div>Update order {orderId}</div>
      <button onClick={() => handleUpdate(orderId)}>update</button>
      <OrderTrackingDemo />
      <OrderTrackingDemo />
      <OrderTrackingDemo />
      <OrderTrackingDemo />
      <hr></hr>
      <UserOrder userId="123423" />
      <UserOrder userId="user-123" />
      <hr></hr>
      <button onClick={() => handleUpdateStock("product-1")}>
        Update stock product - 1
      </button>
      <button onClick={() => handleUpdateStock("product-2")}>
        Update stock product - 2
      </button>
      <ProductViews productId="product-1" />
      <ProductViews productId="product-1" />
      <ProductViews productId="product-1" />
      <ProductViews productId="product-2" />
      <ProductViews productId="product-2" />
    </div>
  );
};

export default AdminPage;
