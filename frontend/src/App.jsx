import { useApolloClient, useMutation, useQuery } from "@apollo/client";
import { Link, NavLink, Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { LOGOUT, ME } from "./graphql.js";
import Login from "./pages/Login.jsx";
import Search from "./pages/Search.jsx";
import OrderDetail from "./pages/OrderDetail.jsx";
import Orders from "./pages/Orders.jsx";

function RequireUser({ user, children }) {
  return user ? children : <Navigate to="/login" replace />;
}

export default function App() {
  const { data, loading } = useQuery(ME);
  const client = useApolloClient();
  const navigate = useNavigate();
  const [logout] = useMutation(LOGOUT);
  const user = data?.me;

  const onLogout = async () => {
    await logout();
    await client.resetStore(); // forget everything cached for the previous user
    navigate("/login");
  };

  if (loading) return <div className="boot">Loading…</div>;

  return (
    <div className="shell">
      <header className="topbar">
        <Link to="/" className="brand">
          <span className="brand-mark">✈</span> WanderSync
        </Link>
        {user && (
          <nav className="nav">
            <NavLink to="/" end>Search</NavLink>
            <NavLink to="/orders">My orders</NavLink>
            <span className="who">{user.fullName}</span>
            <button className="link" onClick={onLogout}>Log out</button>
          </nav>
        )}
      </header>
      <main className="content">
        <Routes>
          <Route path="/login" element={user ? <Navigate to="/" replace /> : <Login />} />
          <Route path="/" element={<RequireUser user={user}><Search /></RequireUser>} />
          <Route path="/orders" element={<RequireUser user={user}><Orders /></RequireUser>} />
          <Route path="/orders/:id" element={<RequireUser user={user}><OrderDetail /></RequireUser>} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}
