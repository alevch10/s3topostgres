from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class EventSchema(BaseModel):
    insert_id: str = Field(alias="$insert_id")
    insert_key: Optional[str] = Field(alias="$insert_key", default=None)
    schema: Optional[str] = Field(alias="$schema", default=None)
    adid: Optional[str] = None
    amplitude_attribution_ids: Optional[List[str]] = None
    amplitude_event_type: Optional[str] = None
    amplitude_id: Optional[int] = None
    app: Optional[int] = None
    city: Optional[str] = None
    client_event_time: datetime
    client_upload_time: Optional[datetime] = None
    country: Optional[str] = None
    data_type: Optional[str] = None
    device_brand: Optional[str] = None
    device_carrier: Optional[str] = None
    device_family: Optional[str] = None
    device_id: Optional[str] = None
    device_manufacturer: Optional[str] = None
    device_model: Optional[str] = None
    device_type: Optional[str] = None
    dma: Optional[str] = None
    event_id: Optional[int] = None
    event_time: Optional[datetime] = None
    event_type: Optional[str] = None
    global_user_properties: Optional[str] = None
    idfa: Optional[str] = None
    ip_address: Optional[str] = None
    is_attribution_event: Optional[str] = None
    language: Optional[str] = None
    library: Optional[str] = None
    location_lat: Optional[float] = None
    location_lng: Optional[float] = None
    os_name: Optional[str] = None
    os_version: Optional[str] = None
    partner_id: Optional[str] = None
    paying: Optional[str] = None
    platform: Optional[str] = None
    processed_time: Optional[datetime] = None
    region: Optional[str] = None
    sample_rate: Optional[float] = None
    server_received_time: Optional[datetime] = None
    server_upload_time: Optional[datetime] = None
    session_id: Optional[int] = None
    source_id: Optional[str] = None
    start_version: Optional[str] = None
    user_creation_time: Optional[datetime] = None
    user_id: Optional[str] = None
    uuid: Optional[str] = None
    version_name: Optional[str] = None
    data_json: Optional[Dict[str, Any]] = Field(alias="data", default=None)
    event_properties_json: Optional[Dict[str, Any]] = Field(
        alias="event_properties", default=None
    )
    group_properties_json: Optional[Dict[str, Any]] = Field(
        alias="group_properties", default=None
    )
    groups_json: Optional[Dict[str, Any]] = Field(alias="groups", default=None)
    plan_json: Optional[Dict[str, Any]] = Field(alias="plan", default=None)
    user_properties_json: Optional[Dict[str, Any]] = Field(
        alias="user_properties", default=None
    )
    extra_json: Optional[Dict[str, Any]] = None  # Для неизвестных полей

    class Config:
        populate_by_name = True  # Для алиасов ($insert_id)
        json_encoders = {datetime: lambda v: v.isoformat()}
        extra = "allow"  # Разрешает extra поля для extra_json
