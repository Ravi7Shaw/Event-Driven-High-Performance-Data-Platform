from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

Quantity = Annotated[int, Field(strict=True, ge=0, le=2_147_483_647)]
Price = Annotated[Decimal, Field(ge=0, max_digits=12, decimal_places=2, allow_inf_nan=False)]
Name = Annotated[str, Field(min_length=1, max_length=200, pattern=r"\S")]


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CreateItem(RequestModel):
    sku: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    name: Name
    description: str | None = Field(None, max_length=4000)
    quantity: Quantity
    unit_price: Price


class UpdateItem(RequestModel):
    name: Name | None = None
    description: str | None = Field(None, max_length=4000)
    unit_price: Price | None = None

    @model_validator(mode="after")
    def valid_patch(self):
        if not self.model_fields_set:
            raise ValueError("At least one update field is required")
        for field in ("name", "unit_price"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        return self


class InventoryChange(RequestModel):
    quantity: int = Field(strict=True, gt=0, le=2_147_483_647)
