from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    UUID,
    JSON,
    BigInteger,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import (INET)


Base = declarative_base()


class WebEvent(Base):
    __tablename__ = "web"

    insert_id = Column(String, primary_key=True)
    insert_key = Column(String)
    schema = Column(String)
    adid = Column(String)
    amplitude_attribution_ids = Column(JSON)
    amplitude_event_type = Column(String)
    amplitude_id = Column(BigInteger)
    app = Column(Integer)
    city = Column(String)
    client_event_time = Column(DateTime, nullable=False)
    client_upload_time = Column(DateTime)
    country = Column(String)
    data_type = Column(String)
    device_brand = Column(String)
    device_carrier = Column(String)
    device_family = Column(String)
    device_id = Column(String)
    device_manufacturer = Column(String)
    device_model = Column(String)
    device_type = Column(String)
    dma = Column(String)
    event_id = Column(Integer)
    event_time = Column(DateTime)
    event_type = Column(String)
    global_user_properties = Column(String)
    idfa = Column(String)
    ip_address = Column(String)
    is_attribution_event = Column(String)
    language = Column(String)
    library = Column(String)
    location_lat = Column(Float)
    location_lng = Column(Float)
    os_name = Column(String)
    os_version = Column(String)
    partner_id = Column(String)
    paying = Column(String)
    platform = Column(String)
    processed_time = Column(DateTime)
    region = Column(String)
    sample_rate = Column(Float)
    server_received_time = Column(DateTime)
    server_upload_time = Column(DateTime)
    session_id = Column(BigInteger)
    source_id = Column(String)
    start_version = Column(String)
    user_creation_time = Column(DateTime)
    user_id = Column(String)
    uuid = Column(String)
    version_name = Column(String)
    data_json = Column(JSON)
    event_properties_json = Column(JSON)
    group_properties_json = Column(JSON)
    groups_json = Column(JSON)
    plan_json = Column(JSON)
    user_properties_json = Column(JSON)
    extra_json = Column(JSON)


class MpEvent(Base):
    __tablename__ = "mp"

    uuid = Column(String, primary_key=True, nullable=False)
    city = Column(String)
    country = Column(String)
    device_id = Column(String, ForeignKey("mobile_devices.device_id"))
    event_id = Column(Integer)
    event_time = Column(DateTime)
    event_type = Column(String)
    language = Column(String)
    os_name = Column(String)
    os_version = Column(String)
    platform = Column(String)
    region = Column(String)
    session_id = Column(BigInteger)
    start_version = Column(String)
    user_id = Column(String)
    version_name = Column(String)
    data_json = Column(JSON)
    event_properties_json = Column(JSON)
    group_properties_json = Column(JSON)
    groups_json = Column(JSON)
    plan_json = Column(JSON)
    user_properties_json = Column(JSON)

    mobile_device = relationship("MobileDevises", back_populates="mp_events", uselist=False)
    user_location = relationship("UserLocations", back_populates="mp_event", uselist=False)
    user_ip = relationship("UserIP", back_populates="mp_event", uselist=False)
    techical_data = relationship("TechnicalData", back_populates="mp_event", uselist=False)

class MobileDevices(Base):
    __tablename__ = "mobile_devices"

    device_id = Column(String, primary_key=True)
    device_brand = Column(String)
    device_carrier = Column(String)
    device_family = Column(String)
    device_manufacturer = Column(String)
    device_model = Column(String)
    device_type = Column(String)

    mp_events = relationship("MpEvent", back_populates="mobile_device")


class UserLocations(Base):
    __tablename__ = "user_locations"

    uuid = Column(String, ForeignKey("mp.uuid"),  primary_key=True)
    location_lat = Column(Float)
    location_lng = Column(Float)

    mp_event = relationship("MpEvent", back_populates="user_location")


class UserIP(Base):
    __tablename__ = "user_ip"

    uuid = Column(String, ForeignKey("mp.uuid"),  primary_key=True)
    ip_address = Column(String)

    mp_event = relationship("MpEvent", back_populates="user_ip")


class TechnicalData(Base):
    __tablename__ = "techical_data"

    uuid = Column(String, ForeignKey("mp.uuid"),  primary_key=True)
    insert_id = Column(String)
    amplitude_attribution_ids = Column(JSON)
    amplitude_id = Column(BigInteger)
    is_attribution_event = Column(String)
    library = Column(String)

    mp_event = relationship("MpEvent", back_populates="techical_data")

# class UserAttributes(Base):
#     __tablename__ = "user_attributes"
#     user_id = Column(String, nullable=False, primary_key=True)
#     cohort_day = Column(Integer, nullable=True)
#     source = Column(String, nullable=True)
#     gender = Column(String, nullable=True)
#     cohort_month = Column(Integer, nullable=True)
#     cohort_week = Column(Integer, nullable=True)
#     registered_via_app = Column(String, nullable=True)

# class UserProperties(Base):
#     __tablename__ = "user_properties"
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     property_key = Column(String, nullable=False, unique=True)
#     data_type = Column(String, nullable=False)
#     description = Column(String, nullable=True)

#     values = relationship("UserPropertyValues", back_populates="property")

# class UserPropertyValues(Base):
#     __tablename__ = "user_property_values"

#     user_id = Column(String)
#     property_id = Column(Integer, ForeignKey("user_properties.id"))
#     value = Column(String, nullable=True)
#     updated_at = Column(DateTime(timezone=True))
#     property = relationship("UserProperties", back_populates="values")
