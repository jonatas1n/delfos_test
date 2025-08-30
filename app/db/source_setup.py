import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from sqlalchemy import Column, DateTime, Float
from sqlalchemy.orm import declarative_base, Session

MEAN_WIND_SPEED = 8.0
WIND_SPEED_STD_DEV = 3.0
MIN_WIND_SPEED = 0.0

MIN_AMBIENT_TEMP = 15.0
MAX_AMBIENT_TEMP = 35.0

MAX_POWER = 3000.0
WIND_POWER_FACTOR = 1.5
MIN_POWER = 0.0
POWER_VARIATION_FACTOR = 0.05


BaseSource = declarative_base()


class SourceData(BaseSource):
    __tablename__ = "data"

    timestamp = Column(DateTime(timezone=True), primary_key=True)
    wind_speed = Column(Float, nullable=False)
    power = Column(Float, nullable=False)
    ambient_temperature = Column(Float, nullable=False)


def create_source_schema(engine) -> None:
    BaseSource.metadata.create_all(engine)


def generate_source_data(start_ts: datetime, end_ts: datetime) -> List[SourceData]:
    minute = timedelta(minutes=1)
    timestamps = []
    curr = start_ts
    while curr < end_ts:
        timestamps.append(curr)
        curr += minute
    return timestamps


def seed_source_data(engine) -> Tuple[datetime, datetime, int]:
    now_utc = datetime.now(timezone.utc)
    end_ts = now_utc.replace(second=0, microsecond=0)
    start_ts = end_ts - timedelta(days=10)

    minute = timedelta(minutes=1)
    timestamps = []
    curr = start_ts
    while curr < end_ts:
        timestamps.append(curr)
        curr += minute

    rows = []
    for ts in timestamps:
        wind_speed = max(
            MIN_WIND_SPEED, random.gauss(MEAN_WIND_SPEED, WIND_SPEED_STD_DEV)
        )
        ambient_temperature = random.uniform(MIN_AMBIENT_TEMP, MAX_AMBIENT_TEMP)
        base_power = min(MAX_POWER, (wind_speed**3) * WIND_POWER_FACTOR)
        power = max(
            MIN_POWER, random.gauss(base_power, base_power * POWER_VARIATION_FACTOR)
        )
        rows.append(
            SourceData(
                timestamp=ts,
                wind_speed=float(wind_speed),
                power=float(power),
                ambient_temperature=float(ambient_temperature),
            )
        )

    with Session(engine) as session:
        existing = session.query(SourceData).limit(1).first()
        if not existing:
            session.bulk_save_objects(rows)
            session.commit()

    return start_ts, end_ts, len(timestamps)
