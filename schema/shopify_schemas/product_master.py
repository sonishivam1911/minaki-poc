from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field

class ProductSchema(BaseModel):
    """
    Example Pydantic model for a Postgres table named 'products'.
    """
    id: Optional[int] = Field(
        default=None,
        description="Primary key, auto-increment ID.",
        # custom metadata (not built-in Pydantic) for table generation:
    )
    name: str = Field(
        description="Product name.",
        max_length=255,  # can be used to build e.g. VARCHAR(255)
    )
    price: float = Field(
        description="Decimal price.",
        decimal_places=2  # custom metadata
    )
    created_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp for creation.",
        pg_type="TIMESTAMP"  # override default type if desired
    )
