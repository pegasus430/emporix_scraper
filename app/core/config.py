from pydantic import BaseSettings


class Settings(BaseSettings):
    icecat_username: str
    icecat_password: str
    google_bucket_name: str

    class Config:
        env_file = ".env"
