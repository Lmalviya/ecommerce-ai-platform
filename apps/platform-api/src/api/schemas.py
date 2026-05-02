from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from enum import Enum

class ConnectorType(str, Enum):
    POSTGRES = "postgres"
    CSV = "csv"
    JSON = "json"

class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ConnectorProfileCreate(BaseModel):
    name: str
    type: ConnectorType
    config: Dict[str, Any]

class ConnectorProfile(ConnectorProfileCreate):
    id: UUID
    created_at: datetime

class IngestionJobCreate(BaseModel):
    profile_id: UUID
    batch_size: Optional[int] = 50
    limit: Optional[int] = 1000

class IngestionJob(BaseModel):
    id: UUID
    profile_id: UUID
    status: JobStatus
    total_items: int
    processed_items: int
    failed_items: int
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

class JobStatusUpdate(BaseModel):
    status: JobStatus
    total_items: Optional[int] = None
    error_message: Optional[str] = None

class JobProgressUpdate(BaseModel):
    processed_increment: int
    failed_increment: int
