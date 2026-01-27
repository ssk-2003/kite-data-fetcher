from typing import Generator
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from sqlalchemy.engine import Engine
from ml_engine.core.config import JWT_SECRET, ALGORITHM
from ml_engine.db.database import get_engine
from ml_engine.crud import crud_user
from ml_engine.schemas.token import TokenPayload

oauth2_scheme = OAuth2PasswordBearer(tokenUrl=f"/api/v1/auth/login")

def get_db_engine() -> Generator[Engine, None, None]:
    """
    Dependency that provides a database engine.
    """
    engine = get_engine()
    try:
        yield engine
    finally:
        # Engine cleanup if necessary
        pass

def get_current_user(
    engine: Engine = Depends(get_db_engine),
    token: str = Depends(oauth2_scheme)
) -> dict:
    """
    Dependency to validate JWT and return the current user.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenPayload(sub=email)
    except JWTError:
        raise credentials_exception
    
    user = crud_user.get_user_by_email(engine=engine, email=token_data.sub)
    if user is None:
        raise credentials_exception
    return user
