from pydantic import BaseSettings

class Settings(BaseSettings):
    MONGO_URI: str
    MONGO_DB: str

    EBAY_APP_ID: str
    EBAY_CERT_ID: str
    EBAY_DEV_ID: str
    EBAY_OAUTH_TOKEN: str

    SHOPIFY_API_KEY: str
    SHOPIFY_PASSWORD: str
    SHOPIFY_STORE_URL: str

    class Config:
        env_file = ".env"

settings = Settings()
