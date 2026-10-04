import { useQuery } from "@apollo/client";
import { Link } from "react-router-dom";
import { MY_ORDERS } from "../graphql.js";
import { dateTime, errorMessage, usd } from "../format.js";

export default function Orders() {
  const { data, loading, error } = useQuery(MY_ORDERS, { fetchPolicy: "network-only" });

  if (loading) return <p className="hint">Loading orders…</p>;
  if (error) return <p className="alert">{errorMessage(error)}</p>;
  const orders = data?.myOrders ?? [];

  return (
    <div className="orders-page">
      <h1>My orders</h1>
      {orders.length === 0 ? (
        <div className="card empty">
          <p>You have no bookings yet.</p>
          <Link to="/" className="primary">Search a trip</Link>
        </div>
      ) : (
        <div className="card table-wrap">
          <table className="orders">
            <thead>
              <tr><th>Created</th><th>Dates</th><th>Total</th><th>Status</th><th></th></tr>
            </thead>
            <tbody>
              {orders.map((o) => (
                <tr key={o.id}>
                  <td>{dateTime(o.createdAt)}</td>
                  <td>{o.checkIn} → {o.checkOut}</td>
                  <td>{usd(o.totalAmount)}</td>
                  <td><span className={`badge ${o.status.toLowerCase()}`}>{o.status}</span></td>
                  <td><Link to={`/orders/${o.id}`}>Details</Link></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
