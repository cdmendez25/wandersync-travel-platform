import { useState } from "react";
import { useApolloClient, useMutation } from "@apollo/client";
import { LOGIN, ME, REGISTER } from "../graphql.js";
import { errorMessage } from "../format.js";

export default function Login() {
  const [mode, setMode] = useState("login");
  const [form, setForm] = useState({ email: "", password: "", fullName: "" });
  const client = useApolloClient();
  const [login, loginState] = useMutation(LOGIN);
  const [register, registerState] = useMutation(REGISTER);
  const busy = loginState.loading || registerState.loading;
  const error = mode === "login" ? loginState.error : registerState.error;

  const update = (field) => (e) => setForm({ ...form, [field]: e.target.value });

  const submit = async (e) => {
    e.preventDefault();
    try {
      if (mode === "login") {
        await login({ variables: { email: form.email, password: form.password } });
      } else {
        await register({ variables: form });
      }
      await client.refetchQueries({ include: [ME] });
    } catch {
      /* shown below */
    }
  };

  return (
    <div className="auth">
      <div className="auth-intro">
        <h1>Flight, hotel and car in one booking.</h1>
        <p>
          WanderSync books the whole trip together. If any part fails, everything already reserved is
          cancelled and refunded automatically.
        </p>
      </div>
      <form className="card auth-card" onSubmit={submit}>
        <div className="tabs" role="tablist">
          <button type="button" role="tab" aria-selected={mode === "login"} onClick={() => setMode("login")}>
            Log in
          </button>
          <button type="button" role="tab" aria-selected={mode === "register"} onClick={() => setMode("register")}>
            Create account
          </button>
        </div>
        {mode === "register" && (
          <label>
            Full name
            <input id="fullName" value={form.fullName} onChange={update("fullName")} required maxLength={100} />
          </label>
        )}
        <label>
          Email
          <input id="email" type="email" autoComplete="email" value={form.email} onChange={update("email")} required />
        </label>
        <label>
          Password
          <input
            id="password"
            type="password"
            autoComplete={mode === "login" ? "current-password" : "new-password"}
            minLength={mode === "register" ? 10 : undefined}
            value={form.password}
            onChange={update("password")}
            required
          />
          {mode === "register" && <small>At least 10 characters.</small>}
        </label>
        {error && <p className="alert">{errorMessage(error)}</p>}
        <button className="primary" disabled={busy}>
          {busy ? "Please wait…" : mode === "login" ? "Log in" : "Create account"}
        </button>
      </form>
    </div>
  );
}
