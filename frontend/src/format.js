export const AIRPORTS = {
  BOG: "Bogotá",
  MDE: "Medellín",
  CTG: "Cartagena",
  CLO: "Cali",
  SMR: "Santa Marta",
  ADZ: "San Andrés",
};

const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
export const usd = (value) => money.format(Number(value || 0));

export const time = (iso) =>
  new Date(iso).toLocaleTimeString("es-CO", { timeZone: "America/Bogota", hour: "2-digit", minute: "2-digit" });

export const dateTime = (iso) =>
  new Date(iso).toLocaleString("es-CO", { timeZone: "America/Bogota", dateStyle: "medium", timeStyle: "short" });

export const addDays = (isoDate, days) => {
  const d = new Date(`${isoDate}T12:00:00`);
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
};

export const today = () => new Date().toISOString().slice(0, 10);

export const errorMessage = (error) => error?.graphQLErrors?.[0]?.message || error?.message || "Something went wrong.";
