from datetime import datetime, date
from typing import List, Optional
from pydantic import BaseModel


class DealerData(BaseModel):
    hash: Optional[str] = None
    dealership_id: str
    vin: str
    mileage: Optional[int] = None
    is_new: Optional[bool] = None
    stock_number: Optional[str] = None
    dealer_year: Optional[int] = None
    dealer_make: Optional[str] = None
    dealer_model: Optional[str] = None
    dealer_trim: Optional[str] = None
    dealer_model_number: Optional[str] = None
    dealer_msrp: Optional[int] = None
    dealer_invoice: Optional[int] = None
    dealer_body: Optional[str] = None
    dealer_inventory_entry_date: Optional[date] = None
    dealer_exterior_color_description: Optional[str] = None
    dealer_interior_color_description: Optional[str] = None
    dealer_exterior_color_code: Optional[str] = None
    dealer_interior_color_code: Optional[str] = None
    dealer_transmission_name: Optional[str] = None
    dealer_installed_option_codes: Optional[List[str]] = []
    dealer_installed_option_descriptions: Optional[List[str]] = []
    dealer_additional_specs: Optional[str]
    dealer_doors: Optional[str]
    dealer_drive_type: Optional[str]
    updated_at: datetime
    dealer_images: Optional[List[str]] = []
    dealer_certified: Optional[bool]

    def __getitem__(self, item):
        return getattr(self, item)
