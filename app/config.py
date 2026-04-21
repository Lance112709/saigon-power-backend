import os

class Settings:
    supabase_url: str       = os.environ.get("SUPABASE_URL", "")
    supabase_anon_key: str  = os.environ.get("SUPABASE_ANON_KEY", "")
    supabase_service_key: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
    anthropic_api_key: str  = os.environ.get("ANTHROPIC_API_KEY", "")
    frontend_url: str       = os.environ.get("FRONTEND_URL", "http://localhost:3000")
    jwt_secret: str         = os.environ.get("JWT_SECRET", "saigon-power-secret-key-change-in-production")

settings = Settings()
