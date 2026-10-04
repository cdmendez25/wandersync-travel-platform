import os

DATABASE_URL = os.environ["DATABASE_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
ORDERS_URL = os.getenv("ORDERS_URL", "http://orders-service:8000")

# Sessions
SESSION_COOKIE = "wsid"
SESSION_IDLE_SECONDS = int(os.getenv("SESSION_IDLE_SECONDS", "1800"))         # 30 min of inactivity
SESSION_ABSOLUTE_SECONDS = int(os.getenv("SESSION_ABSOLUTE_SECONDS", "28800"))  # 8 h max lifetime
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"          # true behind HTTPS

# Rate limits: (max requests, window in seconds)
RATE_GLOBAL = (int(os.getenv("RATE_GLOBAL_PER_MIN", "120")), 60)  # every /graphql request, per IP
RATE_LOGIN_IP = (5, 60)           # login attempts per IP
RATE_LOGIN_EMAIL = (10, 900)      # login attempts per account
RATE_REGISTER_IP = (3, 600)       # sign-ups per IP
RATE_CHECKOUT_USER = (5, 60)      # bookings (payment + checkout) per user

# Account lockout after repeated wrong passwords
MAX_FAILED_LOGINS = 5
LOCKOUT_MINUTES = 15

FRONTEND_ORIGINS = [o.strip() for o in os.getenv("FRONTEND_ORIGINS", "http://localhost:5173").split(",") if o.strip()]
GRAPHIQL = os.getenv("GRAPHIQL", "true").lower() == "true"
ALLOW_FAILURE_SIMULATION = os.getenv("ALLOW_FAILURE_SIMULATION", "true").lower() == "true"

CITY_BY_AIRPORT = {
    "BOG": "Bogotá", "MDE": "Medellín", "CTG": "Cartagena",
    "CLO": "Cali", "SMR": "Santa Marta", "ADZ": "San Andrés",
}
