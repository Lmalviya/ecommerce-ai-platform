import json
from fastapi import APIRouter, HTTPException, Depends
from typing import List
from uuid import UUID
import structlog

from src.api.schemas import ConnectorProfileCreate, ConnectorProfile, IngestionJobCreate, IngestionJob, JobStatusUpdate, JobProgressUpdate
from src.database import DatabaseClient
from src.celery_app import app as celery_app # We will assume this is importable or fix source

logger = structlog.get_logger()
router = APIRouter()

@router.post("/connectors", response_model=ConnectorProfile)
async def create_connector(profile: ConnectorProfileCreate):
    client = DatabaseClient.get_instance()
    sql = """
        INSERT INTO connector_profiles (name, type, config)
        VALUES (%(name)s, %(type)s, %(config)s)
        RETURNING *
    """
    params = {
        "name": profile.name,
        "type": profile.type,
        "config": json.dumps(profile.config)
    }
    with client.pool.connection() as conn:
        row = conn.execute(sql, params).fetchone()
        conn.commit()
    
    return row

@router.get("/connectors", response_model=List[ConnectorProfile])
async def list_connectors():
    client = DatabaseClient.get_instance()
    with client.pool.connection() as conn:
        rows = conn.execute("SELECT * FROM connector_profiles ORDER BY created_at DESC").fetchall()
    return rows

@router.post("/jobs", response_model=IngestionJob)
async def trigger_job(payload: IngestionJobCreate):
    client = DatabaseClient.get_instance()
    
    # 1. Verify connector exists
    with client.pool.connection() as conn:
        profile = conn.execute(
            "SELECT * FROM connector_profiles WHERE id = %s", (payload.profile_id,)
        ).fetchone()
    
    if not profile:
        raise HTTPException(status_code=404, detail="Connector profile not found")

    # 2. Create the Job record
    sql = """
        INSERT INTO ingestion_jobs (profile_id, status)
        VALUES (%(profile_id)s, 'PENDING')
        RETURNING *
    """
    with client.pool.connection() as conn:
        job = conn.execute(sql, {"profile_id": payload.profile_id}).fetchone()
        conn.commit()

    # 3. Dispatch to Celery (The Orchestrator Task)
    # We pass the job_id so the worker can update the status
    celery_app.send_task(
        "start_bulk_ingestion",
        args=[profile["type"], profile["config"]],
        kwargs={
            "job_id": str(job["id"]),
            "batch_size": payload.batch_size,
            "limit": payload.limit
        }
    )
    
    logger.info("ingestion_job_dispatched", job_id=str(job["id"]), profile_id=str(profile["id"]))
    return job

@router.get("/jobs/{job_id}", response_model=IngestionJob)
async def get_job_status(job_id: UUID):
    client = DatabaseClient.get_instance()
    with client.pool.connection() as conn:
        job = conn.execute(
            "SELECT * FROM ingestion_jobs WHERE id = %s", (job_id,)
        ).fetchone()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job

@router.patch("/jobs/{job_id}/status", response_model=IngestionJob)
async def update_job_status(job_id: UUID, payload: JobStatusUpdate):
    client = DatabaseClient.get_instance()
    updates = ["status = %(status)s", "updated_at = NOW()"]
    params = {"status": payload.status, "job_id": job_id}
    
    if payload.total_items is not None:
        updates.append("total_items = %(total_items)s")
        params["total_items"] = payload.total_items
        
    if payload.error_message:
        updates.append("error_message = %(error_message)s")
        params["error_message"] = payload.error_message

    sql = f"""
        UPDATE ingestion_jobs 
        SET {', '.join(updates)} 
        WHERE id = %(job_id)s
        RETURNING *
    """
    
    with client.pool.connection() as conn:
        job = conn.execute(sql, params).fetchone()
        conn.commit()
        
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return job

@router.patch("/jobs/{job_id}/progress", response_model=IngestionJob)
async def update_job_progress(job_id: UUID, payload: JobProgressUpdate):
    client = DatabaseClient.get_instance()
    sql = """
        UPDATE ingestion_jobs 
        SET processed_items = processed_items + %(processed_increment)s,
            failed_items = failed_items + %(failed_increment)s,
            updated_at = NOW()
        WHERE id = %(job_id)s
        RETURNING *
    """
    params = {
        "processed_increment": payload.processed_increment,
        "failed_increment": payload.failed_increment,
        "job_id": job_id
    }
    
    with client.pool.connection() as conn:
        job = conn.execute(sql, params).fetchone()
        conn.commit()
        
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return job
