from pydantic_settings import BaseSettings  # 👈 change this import

class Settings(BaseSettings):
    MONGO_URI: str
    MONGO_DB: str

    EBAY_APP_ID: str
    EBAY_CERT_ID: str
    EBAY_DEV_ID: str
    # Legacy manual token - used as fallback before the OAuth flow has run
    EBAY_OAUTH_TOKEN: str = ""
    # RuName from eBay Developer Portal (Auth Accepted URL name)
    EBAY_RUNAME: str = ""

    SHOPIFY_API_KEY: str
    SHOPIFY_PASSWORD: str
    SHOPIFY_STORE_URL: str

    SHOPIFY_API_KEY_PROD: str
    SHOPIFY_PASSWORD_PROD: str
    SHOPIFY_STORE_URL_PROD: str

    OPENAI_API_KEY: str | None = None

    # Minimal UI/API protection for non-public deployments.
    # When set, /admin, /reporting and related APIs require a passkey.
    ADMIN_PASSKEY: str | None = None

    class Config:
        env_file = ".env"

settings = Settings()
