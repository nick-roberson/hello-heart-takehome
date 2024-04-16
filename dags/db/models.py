import hashlib
from typing import Dict, Optional

from pydantic import BaseModel


class ProviderBase(BaseModel):

    # Original fields
    provider_name: str
    address1: str
    address2: Optional[str] = None
    city: str
    county: str
    state_code: str
    zip: int
    order_label: str
    last_report_date: str
    provider_status: str

    # Optional fields
    geocoded_address: Optional[str] = None
    provider_note: Optional[str] = None
    npi: Optional[str] = None
    courses_available: Optional[str] = None
    national_drug_code: Optional[str] = None

    # Generated fields
    street: str
    state: str
    zip_code: int
    provider_type: str
    latitude: Optional[str] = None
    longitude: Optional[str] = None


class Provider(ProviderBase):
    """Pydantic model for a provider"""

    id: str

    def __init__(self, **data):
        """Initialize the provider object and set its id as a hash of the provider name"""
        # Set the id as the hash
        data["id"] = self.get_id_hash(data)
        # Call the parent constructor
        super().__init__(**data)

    @classmethod
    def get_id_hash(cls, data: Dict) -> str:
        """Generate the id hash for a provider"""
        # Generate the id as a hash based on all values
        excluded_keys = {"id", "latitude", "longitude"}
        hash_args = (v for k, v in data.items() if k not in excluded_keys)
        concatenated_string = "".join(str(arg) for arg in hash_args)

        # Hash the concatenated string using SHA-256
        hashed_bytes = hashlib.sha256(concatenated_string.encode()).digest()

        # Convert the hashed bytes to an integer and then to a string (to match Dynamo Keys)
        hashed_integer = int.from_bytes(hashed_bytes, byteorder="big")
        return str(hashed_integer)

    class Config:
        orm_mode = True
