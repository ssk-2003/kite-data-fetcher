from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import RedirectResponse
from sqlalchemy.engine import Engine
from ml_engine.core.security import create_access_token, verify_password
from ml_engine.core.config import (
    GOOGLE_CLIENT_ID, 
    GOOGLE_CLIENT_SECRET, 
    GOOGLE_REDIRECT_URI,
    FRONTEND_URL
)
from ml_engine.api import deps
from ml_engine.crud import crud_user
from ml_engine.schemas.user import User, UserCreate, PasswordChange
from ml_engine.schemas.token import Token
import httpx
from urllib.parse import urlencode

router = APIRouter()

# Google OAuth URLs
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

@router.post("/signup", response_model=User)
def signup(
    user_in: UserCreate,
    engine: Engine = Depends(deps.get_db_engine)
):
    """
    Create a new user account.
    """
    user = crud_user.get_user_by_email(engine=engine, email=user_in.email)
    if user:
        raise HTTPException(
            status_code=400,
            detail="The user with this username already exists in the system.",
        )
    user = crud_user.create_user(engine=engine, user_in=user_in)
    return user

@router.post("/login", response_model=Token)
def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    engine: Engine = Depends(deps.get_db_engine)
):
    """
    OAuth2 compatible token login, retrieve an access token for future requests.
    """
    user = crud_user.get_user_by_email(engine=engine, email=form_data.username)
    if not user:
        from ml_engine.crud import crud_waitlist
        waitlist_entry = crud_waitlist.get_waitlist_entry(engine=engine, email=form_data.username)
        if waitlist_entry and not waitlist_entry.get("converted_to_user"):
            raise HTTPException(
                status_code=400,
                detail="You're on the waitlist! Please complete signup first to activate your account."
            )
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    if not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=400, detail="Incorrect email or password")
    elif not user["is_active"]:
        raise HTTPException(status_code=400, detail="Inactive user")
    
    access_token = create_access_token(subject=user["email"])
    return {
        "access_token": access_token,
        "token_type": "bearer",
    }

@router.get("/google/login")
def google_login():
    """
    Redirect to Google OAuth consent screen.
    """
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "consent",
    }
    url = f"{GOOGLE_AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url=url)

@router.get("/google/callback")
async def google_callback(
    code: str,
    engine: Engine = Depends(deps.get_db_engine)
):
    """
    Handle Google OAuth callback, exchange code for tokens, and create/authenticate user.
    """
    # Exchange authorization code for tokens
    async with httpx.AsyncClient() as client:
        token_response = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": GOOGLE_REDIRECT_URI,
                "grant_type": "authorization_code",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to get access token from Google: {token_response.text}"
            )
        
        token_data = token_response.json()
        google_access_token = token_data.get("access_token")
        
        # Get user info from Google
        userinfo_response = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {google_access_token}"}
        )
        
        if userinfo_response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail="Failed to get user info from Google"
            )
        
        userinfo = userinfo_response.json()
    
    email = userinfo.get("email")
    full_name = userinfo.get("name")
    
    if not email:
        raise HTTPException(
            status_code=400,
            detail="Email not provided by Google"
        )
    
    # Get or create user
    user = crud_user.get_or_create_oauth_user(
        engine=engine,
        email=email,
        full_name=full_name
    )
    
    # Create JWT token
    access_token = create_access_token(subject=user["email"])
    
    redirect_url = f"{FRONTEND_URL}/auth/google/callback?token={access_token}"
    return RedirectResponse(url=redirect_url)


@router.post("/change-password")
def change_password(
    password_in: PasswordChange,
    current_user: dict = Depends(deps.get_current_user),
    engine: Engine = Depends(deps.get_db_engine)
):
    """
    Change user password.
    """
    # Verify current password
    user_db = crud_user.get_user_by_email(engine=engine, email=current_user["email"])
    if not user_db or not verify_password(password_in.current_password, user_db["hashed_password"]):
        raise HTTPException(
            status_code=400,
            detail="Incorrect password"
        )
    
    # Update password
    new_hashed_password = get_password_hash(password_in.new_password)
    
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET hashed_password = :hp WHERE id = :id"),
            {"hp": new_hashed_password, "id": current_user["id"]}
        )
            
    return {"message": "Password updated successfully"}

