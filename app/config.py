import os

SUPABASE_URL = "https://larwckswepsgvtsdgthv.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxhcndja3N3ZXBzZ3Z0c2RndGh2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY1NTg5NTgsImV4cCI6MjA5MjEzNDk1OH0.GJjioIyZhEFSXoLSkDjXdhDw5dFUGblct-PR4lsH2pU"
SUPABASE_SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxhcndja3N3ZXBzZ3Z0c2RndGh2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjU1ODk1OCwiZXhwIjoyMDkyMTM0OTU4fQ.e-7pnyTUSj8HQiLVQAP3GgkuU-fxYZy55rtUMsbHlYE"

class Settings:
    supabase_url: str        = SUPABASE_URL
    supabase_anon_key: str   = SUPABASE_ANON_KEY
    supabase_service_key: str = SUPABASE_SERVICE_KEY
    anthropic_api_key: str   = os.environ.get("ANTHROPIC_API_KEY", "")
    frontend_url: str        = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app")
    jwt_secret: str          = os.environ.get("JWT_SECRET", "f31b94388d8f4ba37f743e6734adedadb2878b151788084a869914ddad373f3f")

settings = Settings()
