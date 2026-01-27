from sqlalchemy import text
from sqlalchemy.engine import Engine
from ml_engine.core.security import get_password_hash
from ml_engine.schemas.user import UserCreate

def get_user_by_email(*, engine: Engine, email: str) -> dict | None:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, email, hashed_password, full_name, is_active, approval_status, queue_position FROM users WHERE email = :email"),
            {"email": email}
        ).fetchone()
        if result:
            return dict(result._mapping)
    return None

def create_user(*, engine: Engine, user_in: UserCreate) -> dict:
    """Create a new user. If user is in waitlist, preserve their queue position."""
    hashed_password = get_password_hash(user_in.password)
    with engine.begin() as conn:
        # Check if user is in waitlist - preserve their queue position
        waitlist_entry = conn.execute(
            text("SELECT queue_position, full_name FROM waitlist WHERE email = :email AND converted_to_user = FALSE"),
            {"email": user_in.email}
        ).fetchone()
        
        if waitlist_entry:
            # Use waitlist queue position and mark as converted
            queue_position = waitlist_entry._mapping["queue_position"]
            final_name = user_in.full_name or waitlist_entry._mapping.get("full_name")
            
            conn.execute(
                text("UPDATE waitlist SET converted_to_user = TRUE, converted_at = NOW() WHERE email = :email"),
                {"email": user_in.email}
            )
        else:
            # New user - get next queue position
            max_user_pos = conn.execute(
                text("SELECT COALESCE(MAX(queue_position), 0) FROM users")
            ).scalar()
            max_waitlist_pos = conn.execute(
                text("SELECT COALESCE(MAX(queue_position), 0) FROM waitlist")
            ).scalar()
            queue_position = max(max_user_pos or 0, max_waitlist_pos or 0) + 1
            final_name = user_in.full_name
        
        result = conn.execute(
            text(
                """
                INSERT INTO users (email, hashed_password, full_name, approval_status, queue_position)
                VALUES (:email, :hashed_password, :full_name, 'pending', :queue_position)
                RETURNING id, email, full_name, is_active, approval_status, queue_position
                """
            ),
            {
                "email": user_in.email, 
                "hashed_password": hashed_password, 
                "full_name": final_name,
                "queue_position": queue_position
            }
        ).fetchone()
        return dict(result._mapping)

def get_or_create_oauth_user(*, engine: Engine, email: str, full_name: str | None = None) -> dict:
    """
    Get existing user or create a new OAuth user (without password).
    For OAuth users, we generate a random hashed password they'll never use.
    If user is in waitlist, preserve their queue position.
    """
    user = get_user_by_email(engine=engine, email=email)
    if user:
        return user
    
    import secrets
    random_password = secrets.token_urlsafe(32)
    hashed_password = get_password_hash(random_password)
    
    with engine.begin() as conn:
        # Check if user is in waitlist - preserve their queue position
        waitlist_entry = conn.execute(
            text("SELECT queue_position, full_name FROM waitlist WHERE email = :email AND converted_to_user = FALSE"),
            {"email": email}
        ).fetchone()
        
        if waitlist_entry:
            # Use waitlist queue position and mark as converted
            queue_position = waitlist_entry._mapping["queue_position"]
            wl_name = waitlist_entry._mapping.get("full_name") or full_name
            
            conn.execute(
                text("UPDATE waitlist SET converted_to_user = TRUE, converted_at = NOW() WHERE email = :email"),
                {"email": email}
            )
        else:
            # New user - get next queue position
            max_user_pos = conn.execute(
                text("SELECT COALESCE(MAX(queue_position), 0) FROM users")
            ).scalar()
            max_waitlist_pos = conn.execute(
                text("SELECT COALESCE(MAX(queue_position), 0) FROM waitlist")
            ).scalar()
            queue_position = max(max_user_pos or 0, max_waitlist_pos or 0) + 1
            wl_name = full_name
        
        result = conn.execute(
            text(
                """
                INSERT INTO users (email, hashed_password, full_name, approval_status, queue_position)
                VALUES (:email, :hashed_password, :full_name, 'pending', :queue_position)
                RETURNING id, email, full_name, is_active, approval_status, queue_position
                """
            ),
            {
                "email": email, 
                "hashed_password": hashed_password, 
                "full_name": wl_name,
                "queue_position": queue_position
            }
        ).fetchone()
        return dict(result._mapping)

def get_waitlist_stats(*, engine: Engine, user_email: str) -> dict:
    """
    Get waitlist statistics for a user.
    Returns: approval_status, queue_position, total_in_queue, behind_you
    """
    with engine.connect() as conn:
        # Get user's info
        user = conn.execute(
            text("SELECT approval_status, queue_position FROM users WHERE email = :email"),
            {"email": user_email}
        ).fetchone()
        
        if not user:
            return None
        
        user_data = dict(user._mapping)
        queue_pos = user_data.get('queue_position') or 0
        
        # Get total pending users in queue
        total = conn.execute(
            text("SELECT COUNT(*) FROM users WHERE approval_status = 'pending'")
        ).scalar()
        
        # Get users behind this user (higher queue position = signed up later)
        behind = conn.execute(
            text("SELECT COUNT(*) FROM users WHERE approval_status = 'pending' AND queue_position > :pos"),
            {"pos": queue_pos}
        ).scalar()
        
        return {
            "approval_status": user_data.get('approval_status', 'pending'),
            "queue_position": queue_pos,
            "total_in_queue": total or 0,
            "behind_you": behind or 0
        }


def update_user(*, engine: Engine, user_id: int, full_name: str) -> dict:
    """
    Update user profile information.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE users 
                SET full_name = :full_name
                WHERE id = :id
                RETURNING id, email, full_name, is_active, approval_status, queue_position
            """),
            {"full_name": full_name, "id": user_id}
        ).fetchone()
        
        return dict(result._mapping) if result else None



