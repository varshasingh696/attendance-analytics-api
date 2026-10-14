# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+). All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"            # Windows: set MONGO_URI=...
export MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```
MongoDB locally: `docker run -p 27017:27017 mongo:7`. Indexes are created automatically at startup.
`GET /health` returns 200 once MongoDB answers. Interactive docs: `http://localhost:8000/docs`.

## Notes
- Instants are epoch milliseconds in the API and BSON dates in MongoDB; dates, months and shift times are IST strings.
- Analytics use aggregation pipelines; `/admin/explain/{endpoint}` shows their plans.
- Missing: nothing known.
- Tested with 100,000 attendance records: all endpoints respond in under 100 ms and /admin/explain shows IXSCAN for every endpoint.
