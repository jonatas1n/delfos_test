from typing import List

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship, Session

from .source_setup import SourceData
from .utils import build_measurements_from_source_batch


BaseTarget = declarative_base()


class Signal(BaseTarget):
    __tablename__ = "signal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), unique=True, nullable=False)
    data = relationship(
        "Measurement", back_populates="signal", cascade="all, delete-orphan"
    )


class Measurement(BaseTarget):
    __tablename__ = "data"

    timestamp = Column(DateTime(timezone=True), primary_key=True)
    signal_id = Column(Integer, ForeignKey("signal.id"), primary_key=True)
    value = Column(Float, nullable=False)

    signal = relationship("Signal", back_populates="data")


def create_target_schema(engine) -> None:
    BaseTarget.metadata.create_all(engine)


def ensure_signals_and_seed_target(engine_target, engine_source) -> int:
    signal_names = ["wind_speed", "power", "ambient_temperature"]
    with Session(engine_target) as session_tgt:
        name_to_signal = {s.name: s for s in session_tgt.query(Signal).all()}
        to_create = [Signal(name=n) for n in signal_names if n not in name_to_signal]
        if to_create:
            session_tgt.add_all(to_create)
            session_tgt.commit()
        name_to_signal = {s.name: s for s in session_tgt.query(Signal).all()}

        has_data = session_tgt.query(Measurement).limit(1).first() is not None
        if has_data:
            return 0

        inserted = 0
        with Session(engine_source) as session_src:
            for chunk_start in range(0, 1_000_000_000):
                batch: List[SourceData] = (
                    session_src.query(SourceData)
                    .order_by(SourceData.timestamp)
                    .offset(chunk_start * 5000)
                    .limit(5000)
                    .all()
                )
                if not batch:
                    break

                measurements: list[Measurement] = build_measurements_from_source_batch(
                    batch=batch,
                    name_to_signal=name_to_signal,
                    measurement_cls=Measurement,
                )

                session_tgt.bulk_save_objects(measurements)
                session_tgt.commit()
                inserted += len(measurements)

        return inserted
