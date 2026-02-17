from datetime import datetime, timezone

from pydantic import BaseModel, model_validator


class BaseConfig(BaseModel):
    start: datetime
    end: datetime

    @model_validator(mode="before")
    @classmethod
    def set_utc_defaults(cls, values: dict) -> dict:
        for field in ("start", "end"):
            v = values.get(field)
            if isinstance(v, datetime):
                if v.tzinfo is None:
                    values[field] = v.replace(tzinfo=timezone.utc)
                else:
                    values[field] = v.astimezone(timezone.utc)
        return values

    @model_validator(mode="after")
    def validate_start_before_end(self) -> "BaseConfig":
        if self.start >= self.end:
            raise ValueError("start must be strictly before end")
        return self
