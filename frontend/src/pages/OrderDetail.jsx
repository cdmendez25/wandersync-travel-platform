import { useQuery } from "@apollo/client";
import { Link, useParams } from "react-router-dom";
import { ORDER } from "../graphql.js";
import { dateTime, errorMessage, usd } from "../format.js";

const STEPS = ["PAYMENT", "FLIGHT", "HOTEL", "CAR"];
const STEP_LABEL = { PAYMENT: "Payment", FLIGHT: "Flight", HOTEL: "Hotel", CAR: "Car" };

// Final state of each step, derived from the SAGA log
function stepState(step, log) {
  const rows = log.filter((s) => s.step === step);
  if (rows.some((s) => s.action === "COMPENSATE" && s.status === "SUCCEEDED")) return "compensated";
  if (rows.some((s) => s.action === "COMPENSATE" && s.status === "FAILED")) return "stuck";
  if (rows.some((s) => s.action === "EXECUTE" && s.status === "FAILED")) return "failed";
  if (rows.some((s) => s.action === "EXECUTE" && s.status === "SUCCEEDED")) return "done";
  return "skipped";
}

const STATE_TEXT = {
  done: "Confirmed",
  failed: "Failed",
  compensated: "Undone",
  stuck: "Undo failed",
  skipped: "Not started",
};

const HEADLINE = {
  CONFIRMED: "Your trip is booked.",
  CANCELLED: "The booking failed, and everything was undone.",
  FAILED: "The booking failed and needs manual review.",
  COMPENSATING: "Undoing the reserved parts…",
  PENDING: "Booking in progress…",
};

export default function OrderDetail() {
  const { id } = useParams();
  const { data, loading, error } = useQuery(ORDER, { variables: { id }, fetchPolicy: "network-only" });

  if (loading) return <p className="hint">Loading order…</p>;
  if (error) return <p className="alert">{errorMessage(error)}</p>;
  const order = data?.order;
  if (!order) return <p className="alert">Order not found.</p>;

  const log = order.sagaSteps;

  return (
    <div className="order-page">
      <Link to="/orders" className="back">← My orders</Link>
      <div className={`card order-head status-${order.status.toLowerCase()}`}>
        <div>
          <span className={`badge ${order.status.toLowerCase()}`}>{order.status}</span>
          <h1>{HEADLINE[order.status]}</h1>
          {order.failureReason && <p className="reason">Reason: {order.failureReason}</p>}
        </div>
        <dl className="facts">
          <dt>Total</dt><dd>{usd(order.totalAmount)}</dd>
          <dt>Payment</dt><dd>{order.payment?.status ?? "None"}</dd>
          <dt>Dates</dt><dd>{order.checkIn} → {order.checkOut}</dd>
          <dt>Travelers</dt><dd>{order.passengers}</dd>
        </dl>
      </div>

      <section className="card">
        <h2>SAGA steps</h2>
        <p className="hint">Each part is booked in order. If one fails, the parts already booked are undone in reverse order.</p>
        <ol className="pipeline">
          {STEPS.map((step) => {
            const state = stepState(step, log);
            return (
              <li key={step} className={`stage ${state}`}>
                <span className="stage-name">{STEP_LABEL[step]}</span>
                <span className="stage-state">{STATE_TEXT[state]}</span>
              </li>
            );
          })}
        </ol>
      </section>

      <section className="card">
        <h2>Event log</h2>
        <div className="table-wrap">
          <table className="log">
            <thead>
              <tr><th>#</th><th>Time</th><th>Step</th><th>Action</th><th>Status</th><th>Detail</th></tr>
            </thead>
            <tbody>
              {log.map((s, i) => (
                <tr key={i} className={`${s.action.toLowerCase()} ${s.status.toLowerCase()}`}>
                  <td>{i + 1}</td>
                  <td>{dateTime(s.createdAt)}</td>
                  <td>{STEP_LABEL[s.step]}</td>
                  <td>{s.action === "EXECUTE" ? "Book" : "Undo"}</td>
                  <td><span className={`dot ${s.status.toLowerCase()}`}>{s.status.toLowerCase()}</span></td>
                  <td className="detail">{s.error || ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
