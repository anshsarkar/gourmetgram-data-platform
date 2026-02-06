import os
import uuid
import boto3
from fastapi import FastAPI, Depends, UploadFile, File, Form, HTTPException, status
from sqlalchemy.orm import Session
from typing import Optional
from . import models, database, schemas

# Initialize Database Tables
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(title="GourmetGram API")

# S3 / MinIO Client
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

@app.on_event("startup")
async def ensure_bucket():
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except Exception:
        pass # Bucket likely exists

# --- Users ---
@app.post("/users/", response_model=schemas.UserResponse)
def create_user(user: schemas.UserCreate, db: Session = Depends(database.get_db)):
    # Check if username exists
    existing = db.query(models.User).filter(models.User.username == user.username).first()
    if existing:
        return existing
        
    db_user = models.User(username=user.username)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

# --- Images ---
@app.post("/upload/", response_model=schemas.ImageResponse)
def upload_image(
    user_id: uuid.UUID = Form(...),
    caption: str = Form(None),
    category: str = Form(None),
    file: UploadFile = File(...),
    db: Session = Depends(database.get_db)
):
    # 1. Upload file to MinIO/S3
    file_ext = file.filename.split('.')[-1]
    file_key = f"{uuid.uuid4()}.{file_ext}"
    
    try:
        s3.upload_fileobj(file.file, BUCKET_NAME, file_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 Upload failed: {str(e)}")

    # 2. Construct S3 URL (Mock URL structure for local dev)
    s3_url = f"{os.getenv('S3_ENDPOINT_URL')}/{BUCKET_NAME}/{file_key}"

    # 3. Save to DB
    db_image = models.Image(
        user_id=user_id,
        s3_url=s3_url,
        caption=caption,
        category=category
    )
    db.add(db_image)
    db.commit()
    db.refresh(db_image)
    return db_image

# --- Comments ---
@app.post("/comments/", response_model=schemas.CommentResponse)
def add_comment(comment: schemas.CommentCreate, db: Session = Depends(database.get_db)):
    db_comment = models.Comment(
        image_id=comment.image_id,
        user_id=comment.user_id,
        content=comment.content
    )
    db.add(db_comment)
    db.commit()
    db.refresh(db_comment)
    return db_comment

# --- Flags ---
@app.post("/flags/", response_model=schemas.FlagResponse)
def create_flag(flag: schemas.FlagCreate, db: Session = Depends(database.get_db)):
    if not flag.image_id and not flag.comment_id:
        raise HTTPException(status_code=400, detail="Must flag either an image or a comment")

    db_flag = models.Flag(
        user_id=flag.user_id,
        image_id=flag.image_id,
        comment_id=flag.comment_id,
        reason=flag.reason
    )
    db.add(db_flag)
    db.commit()
    db.refresh(db_flag)
    return db_flag

# --- Views (Helper for Generator) ---
@app.post("/images/{image_id}/view")
def record_view(image_id: uuid.UUID, db: Session = Depends(database.get_db)):
    # Direct DB update for now (simple counter)
    # In production, this would go to Kafka/Redis
    image = db.query(models.Image).filter(models.Image.id == image_id).first()
    if not image:
        raise HTTPException(status_code=404, detail="Image not found")
    
    image.views += 1
    db.commit()
    return {"status": "ok", "views": image.views}