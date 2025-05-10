import uvicorn
from fastapi import FastAPI, Query, HTTPException, Depends
from typing import Optional
from db import get_session
from sqlalchemy.orm import Session

from home_task.schemas import JobPostingStatisticsResponse
from statistic_handler import StatisticHandler

app = FastAPI(title="Job Statistics API")


@app.get("/statistics/")
def get_days_to_hire_statistics(
        standard_job_id: str = Query(..., description="ID of the standard job"),
        country_code: Optional[str] = Query(None, description="Country code (optional)"),
        session: Session = Depends(get_session)
) -> JobPostingStatisticsResponse:
    statistic = StatisticHandler(session).get_statistic(standard_job_id, country_code)

    if not statistic:
        raise HTTPException(status_code=404, detail="Statistic not found")

    return JobPostingStatisticsResponse.from_orm(statistic)





if __name__ == "__main__":
    uvicorn.run(app)