import React from "react";
import ReactDOM from "react-dom/client";
import { ApolloClient, ApolloProvider, HttpLink, InMemoryCache } from "@apollo/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App.jsx";
import "./styles.css";

// The frontend talks only to the GraphQL gateway. `credentials: "include"` sends
// the HttpOnly session cookie; JavaScript never sees or stores the session id.
const client = new ApolloClient({
  link: new HttpLink({
    uri: import.meta.env.VITE_GRAPHQL_URL || "http://localhost:8080/graphql",
    credentials: "include",
  }),
  cache: new InMemoryCache(),
});

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ApolloProvider client={client}>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ApolloProvider>
  </React.StrictMode>
);
