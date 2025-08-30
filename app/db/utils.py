from typing import Iterable, List, Mapping, Any


def build_measurements_from_source_batch(
    batch: Iterable[Any],
    name_to_signal: Mapping[str, Any],
    measurement_cls: Any,
) -> List[Any]:
    measurements: List[Any] = []
    for row in batch:
        measurements.append(
            measurement_cls(
                timestamp=row.timestamp,
                signal_id=name_to_signal["wind_speed"].id,
                value=row.wind_speed,
            )
        )
        measurements.append(
            measurement_cls(
                timestamp=row.timestamp,
                signal_id=name_to_signal["power"].id,
                value=row.power,
            )
        )
        measurements.append(
            measurement_cls(
                timestamp=row.timestamp,
                signal_id=name_to_signal["ambient_temperature"].id,
                value=row.ambient_temperature,
            )
        )
    return measurements
