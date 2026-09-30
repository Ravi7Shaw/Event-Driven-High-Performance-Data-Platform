from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: SecretStr = SecretStr("postgresql://localhost/eventvault")
    app_env: str = "development"
    log_level: str = "INFO"
    cache_capacity: int = Field(512, ge=0, le=1_000_000)
    cache_ttl: float = Field(30, gt=0)
    rate_limit: int = Field(100, ge=1)
    rate_limit_window: float = Field(60, gt=0)
    worker_batch_size: int = Field(100, ge=1, le=1000)
    worker_interval: float = Field(0.2, gt=0)
    worker_max_attempts: int = Field(5, ge=1)
    worker_retry_base: float = Field(0.5, gt=0)
    worker_enabled: bool = True
    db_pool_min: int = Field(2, ge=1)
    db_pool_max: int = Field(20, ge=1, le=500)
    max_body_bytes: int = Field(16384, ge=1024, le=1_048_576)
    ws_poll_interval: float = Field(0.1, gt=0)
    ws_heartbeat: float = Field(15, gt=0)
    ws_send_timeout: float = Field(5, gt=0)
    ws_max_connections: int = Field(1000, ge=1)
    api_token: SecretStr = SecretStr("")
    admin_token: SecretStr = SecretStr("")
    failure_rate: float = Field(0, ge=0, le=1)

    @model_validator(mode="after")
    def validate_settings(self):
        if self.db_pool_min > self.db_pool_max:
            raise ValueError("DB_POOL_MIN must not exceed DB_POOL_MAX")
        if self.app_env not in {"development", "test", "production"}:
            raise ValueError("APP_ENV must be development, test, or production")
        if self.app_env == "production":
            if self.failure_rate:
                raise ValueError("Failure injection is disabled in production")
            tokens = [self.api_token.get_secret_value(), self.admin_token.get_secret_value()]
            if any(len(token) < 32 for token in tokens) or tokens[0] == tokens[1]:
                raise ValueError(
                    "Production requires distinct API_TOKEN and ADMIN_TOKEN (32+ chars)"
                )
        return self
