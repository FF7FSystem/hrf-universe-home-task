# Job Posting Statistics Service

## Running the API Server

1. Make sure the database is running:
```bash
docker-compose up -d
```

2. Start the FastAPI server:
```bash
uvicorn home_task.main:app --reload
```

API will be available at: `http://localhost:8000`

## Updating Statistics

To update statistics in the database, use the script:

```bash
python -m home_task.update_statistic
```

Optional parameters:
- `--threshold`: Minimum number of job postings required for statistics calculation (default: 5)

Example:
```bash
python -m home_task.update_statistic --threshold 10
```

## API Endpoint

To get statistics, use:
- **URL**: `/api/v1/statistics`
- **Method**: GET
- **Parameters**:
  - `standard_job_id` (required): Standard job ID
  - `country_code` (optional): Country code in ISO 3166-1 alpha-2 format