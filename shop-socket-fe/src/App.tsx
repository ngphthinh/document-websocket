import { Link, Route, Routes } from "react-router-dom";
import "./App.css";
import OrderTrackingDemo from "./OrderTrackingDemo";
import AdminPage from "./AdminPage";

function App() {
  return (
    <>
      {/* <ProductViews productId="pro-23821"/> */}

      <nav>
        <Link to={"/"}>ADMIN</Link> {"-------------------"}
        <Link to={"/user"}>USER</Link>
      </nav>

      <Routes>
        <Route path="/" element={<AdminPage/>} />
        <Route path="/user" element={<OrderTrackingDemo />} />
      </Routes>
    </>
  );
}

export default App;
