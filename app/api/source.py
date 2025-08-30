from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.source_session import get_source_session
from db.source_setup import SourceData


router = APIRouter(prefix="/source", tags=["source"])

DEFAULT_VARIABLES = ["wind_speed", "power", "ambient_temperature"]
VARIABLE_TO_COLUMN_MAP = {
    "wind_speed": SourceData.wind_speed,
    "power": SourceData.power,
    "ambient_temperature": SourceData.ambient_temperature.label("ambient_temperature"),
}


class DataQueryResponse(BaseModel):
    timestamp: datetime
    wind_speed: Optional[float] = None
    power: Optional[float] = None
    ambient_temperature: Optional[float] = None


class DataQueryParams(BaseModel):
    start: datetime
    end: datetime
    variables: List[str]

    @classmethod
    def validate_variables(cls, var_list: List[str]) -> List[str]:
        invalid = [var for var in var_list if var not in DEFAULT_VARIABLES]
        if invalid:
            raise ValueError(
                f"Invalid variables: {invalid}. Allowed: {DEFAULT_VARIABLES}"
            )
        return var_list


@router.get("/data", response_model=List[DataQueryResponse])
def get_source_data(
    start: datetime = Query(..., description="Start timestamp (inclusive)"),
    end: datetime = Query(..., description="End timestamp (exclusive)"),
    variables: List[str] = Query(DEFAULT_VARIABLES, description="Variables to return"),
    session: Session = Depends(get_source_session),
):
    try:
        params = DataQueryParams(start=start, end=end, variables=variables)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if params.start >= params.end:
        raise HTTPException(status_code=400, detail="start must be before end")

    selected_cols = [SourceData.timestamp]
    for var in params.variables:
        selected_cols.append(VARIABLE_TO_COLUMN_MAP[var])

    stmt = (
        select(*selected_cols)
        .where(SourceData.timestamp >= params.start)
        .where(SourceData.timestamp < params.end)
        .order_by(SourceData.timestamp)
    )

    rows = session.execute(stmt).all()
    results: List[DataQueryResponse] = []
    for row in rows:
        item = {"timestamp": row[0]}
        for idx, var in enumerate(params.variables, start=1):
            item[var] = row[idx]
        results.append(DataQueryResponse(**item))

    return results
