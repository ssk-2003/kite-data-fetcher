from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class SubscriptionPlanBase(BaseModel):
    name: str
    duration_months: int
    original_price: int
    founding_price: int
    per_month_label: Optional[str] = None
    savings_percent: int = 0
    features: List[str] = []
    badge: Optional[str] = None
    badge_icon: Optional[str] = None
    is_highlighted: bool = False
    display_order: int = 0


class SubscriptionPlan(SubscriptionPlanBase):
    id: int
    is_active: bool = True
    created_at: datetime

    class Config:
        from_attributes = True


class SubscriptionPlanCreate(SubscriptionPlanBase):
    pass


class UserSubscriptionBase(BaseModel):
    plan_id: int
    status: str = "active"
    payment_id: Optional[str] = None
    payment_amount: Optional[int] = None


class UserSubscription(UserSubscriptionBase):
    id: int
    user_id: int
    started_at: datetime
    expires_at: Optional[datetime] = None
    created_at: datetime
    plan: Optional[SubscriptionPlan] = None

    class Config:
        from_attributes = True


class UserSubscriptionCreate(UserSubscriptionBase):
    # Hidden field, set by backend from current_user
    pass


class SubscriptionCreate(BaseModel):
    plan_id: int
    payment_id: Optional[str] = None
    payment_amount: Optional[int] = None


class PlansResponse(BaseModel):
    plans: List[SubscriptionPlan]
    count: int
