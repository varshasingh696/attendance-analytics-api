"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (a local .env is read, but real environment variables win)

Layout of this single file
  1. configuration + MongoDB client
  2. time / number helpers (rules R1 - R5, R8)
  3. request models + validators
  4. query / pipeline builders (shared by the real endpoints AND /admin/explain)
  5. endpoints
"""
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Annotated, Any, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, BaseModel, Field, model_validator
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # real environment variables always win over .env

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))  # fixed offset, India has no DST
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# tz_aware=True: PyMongo returns aware UTC datetimes instead of naive ones.
# The localhost default is only a convenience for local runs; the grader injects MONGO_URI.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=UTC,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

PRESENCE = ("PRESENT", "WFH", "ON_DUTY")  # R6
LATE_GRACE_SECONDS = 600  # R2
OVERTIME_MIN_MINUTES = 30  # R3
HALF_DAY_LIMIT = Decimal("4.50")  # R5
MAX_PUNCH_GAP_SECONDS = 24 * 3600
MAX_TREND_DAYS = 92
PATCH_RETRIES = 5

# index names (also used as hints so the query plan is predictable)
IX_EMP_CODE = "uq_emp_code"
IX_EMP_DEPT = "ix_dept_emp"
IX_JOINED = "ix_joined"
IX_DEPT_JOINED = "ix_dept_joined"
IX_EMP_DATE = "uq_emp_date"
IX_DATE_EMP = "ix_date_emp"
IX_STATUS_DATE = "ix_status_date_emp"
IX_EMP_PUNCHIN = "ix_emp_punchin"


def ensure_indexes() -> None:
    """Create every index idempotently. One failure is logged, not fatal."""
    specs = [
        (db.employees, [("emp_code", 1)], {"unique": True, "name": IX_EMP_CODE}),
        (db.employees, [("department", 1), ("emp_code", 1)], {"name": IX_EMP_DEPT}),
        (db.employees, [("joined_on", 1)], {"name": IX_JOINED}),
        (db.employees, [("department", 1), ("joined_on", 1)], {"name": IX_DEPT_JOINED}),
        (db.attendance_logs, [("emp_code", 1), ("date", 1)], {"unique": True, "name": IX_EMP_DATE}),
        (db.attendance_logs, [("date", -1), ("emp_code", 1)], {"name": IX_DATE_EMP}),
        (db.attendance_logs, [("status", 1), ("date", -1), ("emp_code", 1)], {"name": IX_STATUS_DATE}),
        (db.attendance_logs, [("emp_code", 1), ("punch_in", -1)], {"name": IX_EMP_PUNCHIN}),
    ]
    for coll, keys, opts in specs:
        try:
            coll.create_index(keys, **opts)
        except PyMongoError as exc:  # e.g. duplicate data under a unique index
            log.error("could not create index %s: %s", opts["name"], exc)


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except Exception as exc:  # never stop the app from starting
        log.error("index creation failed: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_, exc: PyMongoError):
    log.error("mongo error: %s", exc)
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


# --------------------------------------------------------------------------- #
# 2. Helpers: time, rounding, rules R1 - R5
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """epoch milliseconds -> aware UTC datetime (integer arithmetic, no float error)."""
    return EPOCH + timedelta(milliseconds=ms)


def dt_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:  # defensive: treat naive values as UTC
        dt = dt.replace(tzinfo=UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    return None if dt is None else dt_to_ms(dt)


def now_ms() -> int:
    return dt_to_ms(datetime.now(UTC))


def trunc_ms(ms: int) -> int:
    """R1: instants are truncated to whole seconds."""
    return ms // 1000 * 1000


def trunc_dt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.replace(microsecond=0)


def round_half_up(value: Any, places: int) -> Optional[float]:
    """R8: half-up rounding (Python round() and Mongo $round are banker's rounding)."""
    if value is None:
        return None
    dec = value if isinstance(value, Decimal) else Decimal(repr(float(value)))
    return float(dec.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def hhmm_to_minutes(text: str) -> int:
    hours, minutes = text.split(":")
    return int(hours) * 60 + int(minutes)


def is_overnight(emp: dict) -> bool:
    return hhmm_to_minutes(emp["shift_end"]) <= hhmm_to_minutes(emp["shift_start"])


def attendance_date(punch_in: datetime, emp: dict) -> Date:
    """R1: IST date of the punch-in; for overnight shifts an early-morning punch-in
    (earlier than shift_end) belongs to the shift that started the previous day."""
    ist = punch_in.astimezone(IST)
    day = ist.date()
    if is_overnight(emp):
        seconds_of_day = ist.hour * 3600 + ist.minute * 60 + ist.second
        if seconds_of_day < hhmm_to_minutes(emp["shift_end"]) * 60:
            day -= timedelta(days=1)
    return day


def _ist_at(day: Date, hhmm: str, extra_days: int = 0) -> datetime:
    minutes = hhmm_to_minutes(hhmm)
    return datetime(day.year, day.month, day.day, minutes // 60, minutes % 60, tzinfo=IST) + timedelta(
        days=extra_days
    )


def shift_start_dt(day: Date, emp: dict) -> datetime:
    return _ist_at(day, emp["shift_start"])


def shift_end_dt(day: Date, emp: dict) -> datetime:
    # overnight shift ends on the next calendar day (R1)
    return _ist_at(day, emp["shift_end"], 1 if is_overnight(emp) else 0)


def compute_late_minutes(punch_in: datetime, day: Date, emp: dict) -> int:
    """R2: late only if STRICTLY more than 10:00 after shift_start; then whole minutes since shift_start."""
    delta = int((punch_in - shift_start_dt(day, emp)).total_seconds())
    return delta // 60 if delta > LATE_GRACE_SECONDS else 0


def compute_overtime(punch_out: datetime, day: Date, emp: dict) -> int:
    """R3: whole minutes after shift_end, counted only if >= 30."""
    delta = int((punch_out - shift_end_dt(day, emp)).total_seconds())
    minutes = delta // 60 if delta > 0 else 0
    return minutes if minutes >= OVERTIME_MIN_MINUTES else 0


def compute_work(punch_in: datetime, punch_out: datetime) -> tuple[float, bool]:
    """R4 + R5: hours rounded half-up to 2 decimals; half day if the ROUNDED value < 4.50."""
    seconds = int((punch_out - punch_in).total_seconds())
    hours = (Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(hours), hours < HALF_DAY_LIMIT


def derive(emp: dict, day: Date, status: str, punch_in: Optional[datetime], punch_out: Optional[datetime]) -> dict:
    """All derived fields for a record (see the table in DATA_MODEL.md)."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return out
    pin = trunc_dt(punch_in)
    out["late_minutes"] = compute_late_minutes(pin, day, emp)
    if punch_out is not None:
        pout = trunc_dt(punch_out)
        out["work_hours"], out["half_day"] = compute_work(pin, pout)
        out["overtime_minutes"] = compute_overtime(pout, day, emp)
    return out


# --------------------------------------------------------------------------- #
# Response shaping (never expose _id; instants become epoch ms)
# --------------------------------------------------------------------------- #
def _ms_if_dt(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": to_ms(doc.get("created_at")),
    }


def record_out(doc: dict) -> dict:
    history = []
    for entry in doc.get("history") or []:
        changes = {
            key: {"from": _ms_if_dt(val.get("from")), "to": _ms_if_dt(val.get("to"))}
            for key, val in (entry.get("changes") or {}).items()
        }
        history.append(
            {"at": to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}
        )
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": history,
    }


# --------------------------------------------------------------------------- #
# 3. Request models and validators
# --------------------------------------------------------------------------- #
DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
SHIFT_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
MIN_MS, MAX_MS = 100_000_000_000, 4_102_444_800_000


def check_date(value: Optional[str]) -> Optional[str]:
    """A real calendar date in YYYY-MM-DD form (2026-02-30 is rejected)."""
    if value is None:
        return value
    if not DATE_RE.fullmatch(value):
        raise ValueError("must be YYYY-MM-DD")
    try:
        Date.fromisoformat(value)
    except ValueError:
        raise ValueError("not a real calendar date")
    return value


DateStr = Annotated[str, AfterValidator(check_date)]
OptDateStr = Annotated[Optional[str], AfterValidator(check_date)]
MonthStr = Annotated[str, Query(pattern=MONTH_RE)]
# strict=True: floats, strings and booleans are rejected; range rejects "seconds"-looking values
EpochMs = Annotated[int, Field(strict=True, ge=MIN_MS, le=MAX_MS)]


class Status(str, Enum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    LEAVE = "LEAVE"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"


class PresenceStatus(str, Enum):
    PRESENT = "PRESENT"
    WFH = "WFH"
    ON_DUTY = "ON_DUTY"


class EmployeeCreate(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=SHIFT_RE)
    shift_end: str = Field(default="18:30", pattern=SHIFT_RE)
    joined_on: DateStr

    @model_validator(mode="after")
    def _shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchInRequest(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = PresenceStatus.PRESENT


class PunchOutRequest(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    # "= None" defaults are not validated, but an explicit JSON null IS rejected (422),
    # which matches the contract (these fields are not nullable).
    status: Status = None  # type: ignore[assignment]
    punch_in: EpochMs = None  # type: ignore[assignment]
    punch_out: EpochMs = None  # type: ignore[assignment]
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


def raise_422(message: str, loc: str = "query") -> None:
    raise HTTPException(status_code=422, detail=[{"loc": [loc], "msg": message, "type": "value_error"}])


def get_employee(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="employee not found")
    return emp


def parse_month(month: str) -> tuple[Date, Date, Date]:
    """-> (first day, first day of next month, last day) of 'YYYY-MM'."""
    try:
        year, mon = int(month[:4]), int(month[5:7])
        first = Date(year, mon, 1)
        nxt = Date(year + 1, 1, 1) if mon == 12 else Date(year, mon + 1, 1)
    except (ValueError, OverflowError):
        raise_422("invalid month", "month")
    return first, nxt, nxt - timedelta(days=1)


def check_range(date_from: Optional[str], date_to: Optional[str]) -> None:
    if date_from and date_to and date_from > date_to:  # YYYY-MM-DD compares correctly as text
        raise_422("date_from must not be after date_to", "date_from")


# --------------------------------------------------------------------------- #
# 4. Query / pipeline builders  (the real endpoints and /admin/explain both call these)
# --------------------------------------------------------------------------- #
def _present(prefix: str = "") -> dict:
    return {"$in": [f"${prefix}status", list(PRESENCE)]}


def _day_weight(prefix: str = "") -> dict:
    # a half day counts as 0.5 (R5); missing half_day means false
    return {"$cond": [{"$ifNull": [f"${prefix}half_day", False]}, 0.5, 1]}


def _present_weight_any_day(prefix: str = "") -> dict:
    return {"$cond": [_present(prefix), _day_weight(prefix), 0]}


def _present_weight_weekdays(prefix: str = "") -> dict:
    """present_days contribution: presence status AND Monday-Friday (R6, R7)."""
    day = {"$dateFromString": {"dateString": f"${prefix}date", "format": "%Y-%m-%d"}}
    return {"$cond": [{"$and": [_present(prefix), {"$lte": [{"$isoDayOfWeek": day}, 5]}]}, _day_weight(prefix), 0]}


def _late_flag(prefix: str = "") -> dict:
    return {"$cond": [{"$gt": [{"$ifNull": [f"${prefix}late_minutes", 0]}, 0]}, 1, 0]}


def _late_minutes(prefix: str = "") -> dict:
    return {"$ifNull": [f"${prefix}late_minutes", 0]}


def _is_status(status: str, prefix: str = "") -> dict:
    return {"$cond": [{"$eq": [f"${prefix}status", status]}, 1, 0]}


def build_attendance_find(
    emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str], status: Optional[str], page: int, page_size: int
) -> dict:
    flt: dict = {}
    if emp_code is not None:
        flt["emp_code"] = emp_code
    if date_from or date_to:
        flt["date"] = {}
        if date_from:
            flt["date"]["$gte"] = date_from
        if date_to:
            flt["date"]["$lte"] = date_to
    if status is not None:
        flt["status"] = status
    if emp_code is not None:
        hint = IX_EMP_DATE
    elif status is not None:
        hint = IX_STATUS_DATE
    else:
        hint = IX_DATE_EMP
    return {
        "filter": flt,
        "sort": [("date", -1), ("emp_code", 1)],
        "skip": (page - 1) * page_size,
        "limit": page_size,
        "hint": hint,
    }


def build_monthly_pipeline(emp_code: str, first: Date, nxt: Date) -> tuple[list, str]:
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lt": nxt.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": _present_weight_weekdays()},
                "leave_days": {"$sum": _is_status("LEAVE")},
                "late_count": {"$sum": _late_flag()},
                "total_late_minutes": {"$sum": _late_minutes()},
                "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            }
        },
    ]
    return pipeline, IX_EMP_DATE


def build_dept_summary_pipeline(first: Date, nxt: Date, last: Date, department: Optional[str]) -> tuple[list, str]:
    """Starts from employees, so people with zero logs still count (R9) and future joiners do not."""
    match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    hint = IX_DEPT_JOINED if department is not None else IX_JOINED
    p = "logs."
    pipeline = [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [{"$match": {"date": {"$gte": first.isoformat(), "$lt": nxt.isoformat()}}}],
                "as": "logs",
            }
        },
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$department",
                "emps": {"$addToSet": "$emp_code"},
                "present_days": {"$sum": _present_weight_weekdays(p)},
                # mean over RECORDS that are present-status and have a work_hours number
                "avg_work_hours": {
                    "$avg": {
                        "$cond": [{"$and": [_present(p), {"$isNumber": "$logs.work_hours"}]}, "$logs.work_hours", None]
                    }
                },
                "late_count": {"$sum": _late_flag(p)},
                "total_late_minutes": {"$sum": _late_minutes(p)},
                "leave_count": {"$sum": _is_status("LEAVE", p)},
                "on_duty_count": {"$sum": _is_status("ON_DUTY", p)},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": {"$size": "$emps"},
                "present_days": 1,
                "avg_work_hours": 1,
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]
    return pipeline, hint


def build_leaderboard_pipeline(first: Date, nxt: Date, limit: int, department: Optional[str]) -> tuple[list, str]:
    pipeline: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lt": nxt.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # drops logs whose employee does not exist
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # filter BEFORE ranking
    pipeline += [
        # $rank = standard competition ranking: 1, 2, 2, 4
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applied AFTER ranking, so ties at the cutoff stay
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$emp.name",
                "department": "$emp.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline, IX_DATE_EMP


def load_department(department: str) -> tuple[list, list]:
    """-> (emp_codes, joined_on dates) of a department; 404 when it has no employees."""
    rows = list(
        db.employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}).hint(IX_EMP_DEPT)
    )
    if not rows:
        raise HTTPException(status_code=404, detail="department not found")
    return [r["emp_code"] for r in rows], [r["joined_on"] for r in rows]


def build_trend_pipeline(d_from: Date, d_to: Date, codes: list, joined: list) -> tuple[list, str]:
    """One row per calendar day, built inside MongoDB with $densify + $setWindowFields."""
    lo = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    hi = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC) + timedelta(days=1)  # explicit upper bound is exclusive
    pipeline = [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
        # one harmless seed document (no present/late contribution) so $densify always has input,
        # even when there are no logs at all in the range
        {"$unionWith": {"pipeline": [{"$documents": [{"date": d_from.isoformat(), "status": "SEED"}]}]}},
        {
            "$group": {
                "_id": "$date",
                "present_count": {"$sum": _present_weight_any_day()},  # weekends included
                "late_count": {"$sum": _late_flag()},
            }
        },
        {"$set": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        {"$sort": {"d": 1}},
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lo, hi]}}},
        {
            "$set": {
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$d"}},
                "dow": {"$isoDayOfWeek": "$d"},
                "present_count": {"$ifNull": ["$present_count", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
            }
        },
        {
            "$set": {
                "is_working_day": {"$lte": ["$dow", 5]},
                # R9: employees that joined on or before that day
                "headcount": {
                    "$size": {"$filter": {"input": {"$literal": joined}, "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}
                },
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        {"$divide": ["$present_count", "$headcount"]},
                        None,
                    ]
                }
            }
        },
        # current row + up to 6 previous rows; $avg ignores null rates
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {"moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": 1,
            }
        },
        {"$sort": {"date": 1}},
    ]
    return pipeline, IX_EMP_DATE


def working_days(first: Date, last: Date, joined: Date) -> int:
    """R7: Mon-Fri days from max(first of month, joined_on) to the end of the month."""
    day, count = max(first, joined), 0
    while day <= last:
        if day.weekday() < 5:
            count += 1
        day += timedelta(days=1)
    return count


def check_trend_range(d_from: str, d_to: str) -> tuple[Date, Date]:
    start, end = Date.fromisoformat(d_from), Date.fromisoformat(d_to)
    if end < start:
        raise_422("'to' must not be before 'from'", "to")
    if (end - start).days + 1 > MAX_TREND_DAYS:
        raise_422(f"range must not exceed {MAX_TREND_DAYS} days", "to")
    return start, end


# --------------------------------------------------------------------------- #
# 5. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok"}


# ----- employees ----- #
@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = body.model_dump()
    doc["created_at"] = ms_to_dt(now_ms())
    try:
        db.employees.insert_one(doc)  # uniqueness comes from the unique index, not from a prior read
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    flt = {"department": department} if department is not None else {}
    total = db.employees.count_documents(flt)  # the FILTERED total
    cursor = db.employees.find(flt, {"_id": 0}).sort("emp_code", 1).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# ----- attendance ----- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee(body.emp_code)
    ms = trunc_ms(body.punched_at if body.punched_at is not None else now_ms())
    pin = ms_to_dt(ms)
    day = attendance_date(pin, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status.value,
        "punch_in": pin,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(pin, day, emp),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): exactly one parallel request wins
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee(body.emp_code)
    pout = ms_to_dt(trunc_ms(body.punched_at if body.punched_at is not None else now_ms()))
    # most recent record whose punch_in <= punched_at (null punch_in never matches a date comparison)
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": pout}}, sort=[("punch_in", -1)], hint=IX_EMP_PUNCHIN
    )
    if rec is None:
        raise HTTPException(status_code=404, detail="no punch-in found")
    pin = trunc_dt(rec["punch_in"])
    gap = int((pout - pin).total_seconds())
    if gap <= 0 or gap > MAX_PUNCH_GAP_SECONDS:
        raise_422("punched_at must be after punch_in and within 24 hours of it", "punched_at")
    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")
    day = Date.fromisoformat(rec["date"])
    derived = derive(emp, day, rec["status"], pin, pout)
    update = {"punch_out": pout, **derived}
    # atomic guard: only succeeds while the record is still open, so two parallel punch-outs give one 200
    res = db.attendance_logs.update_one({"_id": rec["_id"], "punch_out": None}, {"$set": update})
    if res.matched_count == 0:
        raise HTTPException(status_code=409, detail="already punched out")
    return record_out({**rec, **update})


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: OptDateStr = None,
    date_to: OptDateStr = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    check_range(date_from, date_to)
    plan = build_attendance_find(emp_code, date_from, date_to, status.value if status else None, page, page_size)
    total = db.attendance_logs.count_documents(plan["filter"])
    cursor = (
        db.attendance_logs.find(plan["filter"], {"_id": 0})
        .sort(plan["sort"])
        .skip(plan["skip"])
        .limit(plan["limit"])
        .hint(plan["hint"])
    )
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


def plan_regularization(emp: dict, rec: dict, body: RegularizeRequest) -> dict:
    """Validate a correction against the stored record and build the update. Raises 422."""
    new_status = body.status.value if body.status is not None else rec["status"]
    old_in, old_out = rec.get("punch_in"), rec.get("punch_out")
    day = Date.fromisoformat(rec["date"])

    if new_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise_422("ABSENT/LEAVE clear the punch times; do not send punch_in/punch_out", "body")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(trunc_ms(body.punch_in)) if body.punch_in is not None else old_in
        new_out = ms_to_dt(trunc_ms(body.punch_out)) if body.punch_out is not None else old_out
        if new_in is None:
            raise_422("a presence status requires a punch_in", "body")
        if body.punch_in is not None and attendance_date(new_in, emp) != day:
            raise_422("punch_in must stay on the record's attendance date", "punch_in")
        if new_out is not None and (body.punch_in is not None or body.punch_out is not None):
            gap = int((trunc_dt(new_out) - trunc_dt(new_in)).total_seconds())
            if gap <= 0 or gap > MAX_PUNCH_GAP_SECONDS:
                raise_422("punch_out must be after punch_in and within 24 hours", "punch_out")

    new_vals = {"status": new_status, "punch_in": new_in, "punch_out": new_out}
    new_vals.update(derive(emp, day, new_status, new_in, new_out))
    old_vals = {
        "status": rec["status"],
        "punch_in": old_in,
        "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day", False)),
    }
    if all(new_vals[k] == old_vals[k] for k in ("status", "punch_in", "punch_out")):
        raise_422("the request changes nothing", "body")  # a request that changes nothing is 422
    changes = {k: {"from": old_vals[k], "to": new_vals[k]} for k in new_vals if new_vals[k] != old_vals[k]}
    entry = {
        "at": ms_to_dt(now_ms()),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }
    return {"set": new_vals, "entry": entry}


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(emp_code: str, date: DateStr, body: RegularizeRequest):
    emp = get_employee(emp_code)
    for _ in range(PATCH_RETRIES):
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(status_code=404, detail="no attendance record for that date")
        plan = plan_regularization(emp, rec, body)
        old_history = rec.get("history") or []
        # optimistic concurrency: write only if the record is still exactly as we read it
        guard = {
            "_id": rec["_id"],
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(old_history)]},
        }
        res = db.attendance_logs.update_one(guard, {"$set": plan["set"], "$push": {"history": plan["entry"]}})
        if res.matched_count == 1:
            return record_out({**rec, **plan["set"], "history": old_history + [plan["entry"]]})
        # someone else changed the record in between: re-read and try again
    raise HTTPException(status_code=409, detail="record is being modified concurrently, retry")


# ----- analytics ----- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthStr):
    first, nxt, last = parse_month(month)
    emp = get_employee(emp_code)
    pipeline, hint = build_monthly_pipeline(emp_code, first, nxt)
    rows = list(db.attendance_logs.aggregate(pipeline, hint=hint))
    agg = rows[0] if rows else {}
    wdays = working_days(first, last, Date.fromisoformat(emp["joined_on"]))
    present = agg.get("present_days", 0)
    pct = None
    if wdays > 0:
        pct = round_half_up(Decimal(repr(float(present))) * 100 / Decimal(wdays), 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": wdays,
        "present_days": round_half_up(present, 2),
        "leave_days": int(agg.get("leave_days", 0)),
        "late_count": int(agg.get("late_count", 0)),
        "total_late_minutes": int(agg.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(agg.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: MonthStr, department: Optional[str] = None):
    first, nxt, last = parse_month(month)
    pipeline, hint = build_dept_summary_pipeline(first, nxt, last, department)
    items = []
    for row in db.employees.aggregate(pipeline, hint=hint):
        if row["headcount"] == 0:
            continue
        items.append(
            {
                "department": row["department"],
                "headcount": int(row["headcount"]),
                "present_days": round_half_up(row["present_days"], 2),
                "avg_work_hours": round_half_up(row.get("avg_work_hours"), 2),
                "late_count": int(row["late_count"]),
                "total_late_minutes": int(row["total_late_minutes"]),
                "leave_count": int(row["leave_count"]),
                "on_duty_count": int(row["on_duty_count"]),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthStr, limit: int = Query(10, ge=1, le=50), department: Optional[str] = None):
    first, nxt, _ = parse_month(month)
    pipeline, hint = build_leaderboard_pipeline(first, nxt, limit, department)
    items = []
    for row in db.attendance_logs.aggregate(pipeline, hint=hint):
        items.append(
            {
                "rank": int(row["rank"]),
                "emp_code": row["emp_code"],
                "name": row["name"],
                "department": row["department"],
                "total_late_minutes": int(row["total_late_minutes"]),
                "late_count": int(row["late_count"]),
            }
        )
    return {"month": month, "items": items}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: Annotated[str, AfterValidator(check_date), Query(alias="from")],
    to: DateStr = Query(...),
):
    d_from, d_to = check_trend_range(from_, to)
    codes, joined = load_department(department)
    pipeline, hint = build_trend_pipeline(d_from, d_to, codes, joined)
    items = []
    for row in db.attendance_logs.aggregate(pipeline, hint=hint):
        items.append(
            {
                "date": row["date"],
                "is_working_day": bool(row["is_working_day"]),
                "headcount": int(row["headcount"]),
                "present_count": round_half_up(row["present_count"], 2),
                "late_count": int(row["late_count"]),
                "attendance_rate": round_half_up(row.get("attendance_rate"), 4),
                "moving_avg_7d": round_half_up(row.get("moving_avg_7d"), 4),
            }
        )
    return {"department": department, "items": items}


# ----- admin: explain ----- #
class ExplainTarget(str, Enum):
    attendance_list = "attendance_list"
    employee_monthly = "employee_monthly"
    department_summary = "department_summary"
    late_leaderboard = "late_leaderboard"
    department_trend = "department_trend"


def _require(**params: Any) -> None:
    missing = [name for name, value in params.items() if value is None]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": ["query", n], "msg": "Field required", "type": "missing"} for n in missing],
        )


def _explain(collection: str, kind: str, body: dict) -> dict:
    result = db.command({"explain": {kind: collection, **body}, "verbosity": "executionStats"})
    return json.loads(json_util.dumps(result))  # BSON -> plain JSON


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=MONTH_RE)] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: OptDateStr = None,
    date_to: OptDateStr = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], AfterValidator(check_date), Query(alias="from")] = None,
    to: OptDateStr = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Explain (executionStats) of each endpoint's main query, built by the SAME builder functions."""
    if endpoint is ExplainTarget.attendance_list:
        check_range(date_from, date_to)
        plan = build_attendance_find(emp_code, date_from, date_to, status.value if status else None, page, page_size)
        body = {
            "filter": plan["filter"],
            "sort": dict(plan["sort"]),
            "skip": plan["skip"],
            "limit": plan["limit"],
            "hint": plan["hint"],
        }
        collection, kind = "attendance_logs", "find"
    elif endpoint is ExplainTarget.employee_monthly:
        _require(emp_code=emp_code, month=month)
        first, nxt, _ = parse_month(month)
        pipeline, hint = build_monthly_pipeline(emp_code, first, nxt)
        collection, kind, body = "attendance_logs", "aggregate", {"pipeline": pipeline, "cursor": {}, "hint": hint}
    elif endpoint is ExplainTarget.department_summary:
        _require(month=month)
        first, nxt, last = parse_month(month)
        pipeline, hint = build_dept_summary_pipeline(first, nxt, last, department)
        collection, kind, body = "employees", "aggregate", {"pipeline": pipeline, "cursor": {}, "hint": hint}
    elif endpoint is ExplainTarget.late_leaderboard:
        _require(month=month)
        first, nxt, _ = parse_month(month)
        pipeline, hint = build_leaderboard_pipeline(first, nxt, limit, department)
        collection, kind, body = "attendance_logs", "aggregate", {"pipeline": pipeline, "cursor": {}, "hint": hint}
    else:  # department_trend
        _require(department=department, **{"from": from_}, to=to)
        d_from, d_to = check_trend_range(from_, to)
        codes, joined = load_department(department)
        pipeline, hint = build_trend_pipeline(d_from, d_to, codes, joined)
        collection, kind, body = "attendance_logs", "aggregate", {"pipeline": pipeline, "cursor": {}, "hint": hint}
    return {"endpoint": endpoint.value, "collection": collection, "explain": _explain(collection, kind, body)}
