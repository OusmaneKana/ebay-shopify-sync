from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    MONGO_URI: str
    MONGO_DB: str

    # Baserow (optional unless a script requires it)
    BASEROW_API_TOKEN: str | None = None
    BASEROW_BASE_URL: str | None = None
    BASEROW_TABLE_ID: int | None = None

    EBAY_APP_ID: str
    EBAY_CERT_ID: str
    EBAY_DEV_ID: str
    # Legacy manual token - used as fallback before the OAuth flow has run
    EBAY_OAUTH_TOKEN: str = ""
    # RuName from eBay Developer Portal (Auth Accepted URL name)
    EBAY_RUNAME: str = ""

    SHOPIFY_API_KEY_PROD: str
    SHOPIFY_PASSWORD_PROD: str
    SHOPIFY_STORE_URL_PROD: str

    OPENAI_API_KEY: str | None = None

    # Etsy OAuth (optional until Etsy integration is enabled)
    ETSY_CLIENT_ID: str | None = None
    ETSY_CLIENT_SECRET: str | None = None
    ETSY_REDIRECT_URI: str | None = None
    ETSY_CODE_VERIFIER: str | None = None
    ETSY_SCOPES: str = "listings_r listings_w transactions_r transactions_w shops_r shops_w"
    ETSY_TOKEN: str | None = None
    ETSY_WEBHOOK_SIGNING_SECRET: str | None = None
    ETSY_WEBHOOK_TOLERANCE_SECONDS: int = 300

    # Minimal UI/API protection for non-public deployments.
    # When set, /admin, /reporting and related APIs require a passkey.
    ADMIN_PASSKEY: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

settings = Settings()
