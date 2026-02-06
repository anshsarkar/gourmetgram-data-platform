from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime

# --- Users ---
class UserCreate(BaseModel):
    username: str

class UserResponse(BaseModel):
    id: UUID
    username: str
    created_at: datetime
    class Config:
        from_attributes = True

# --- Images ---
# Note: Uploads use Form data, so we don't strictly need a Pydantic model for the input body,
# but we need one for the response.
class ImageResponse(BaseModel):
    id: UUID
    user_id: UUID
    s3_url: str
    caption: Optional[str] = None
    category: Optional[str] = None
    views: int
    uploaded_at: datetime
    class Config:
        from_attributes = True

# --- Comments ---
class CommentCreate(BaseModel):
    image_id: UUID
    user_id: UUID
    content: str

class CommentResponse(BaseModel):
    id: UUID
    image_id: UUID
    user_id: UUID
    content: str
    created_at: datetime
    class Config:
        from_attributes = True

# --- Flags ---
class FlagCreate(BaseModel):
    user_id: UUID
    reason: str
    image_id: Optional[UUID] = None
    comment_id: Optional[UUID] = None

class FlagResponse(BaseModel):
    id: UUID
    reason: str
    created_at: datetime
    class Config:
        from_attributes = True