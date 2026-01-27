from typing import Optional, Literal
from pydantic import BaseModel, EmailStr

ApprovalStatus = Literal['pending', 'approved', 'rejected']

class UserBase(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    is_active: Optional[bool] = True

class UserCreate(UserBase):
    email: EmailStr
    password: str

class UserUpdate(BaseModel):
    full_name: Optional[str] = None

class PasswordChange(BaseModel):
    current_password: str
    new_password: str

class User(UserBase):
    id: Optional[int] = None
    approval_status: Optional[ApprovalStatus] = 'pending'
    queue_position: Optional[int] = None
    subscription: Optional[dict] = None

    class Config:
        from_attributes = True

class WaitlistStatus(BaseModel):
    approval_status: ApprovalStatus
    queue_position: int
    total_in_queue: int
    behind_you: int
