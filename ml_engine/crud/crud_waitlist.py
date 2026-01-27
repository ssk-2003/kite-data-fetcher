from sqlalchemy import text
from sqlalchemy.engine import Engine


def add_to_waitlist(*, engine: Engine, email: str, full_name: str = None) -> dict:
    """Add email to waitlist. Returns existing entry if already exists."""
    with engine.connect() as conn:
        # Check if already a full user FIRST
        user = conn.execute(
            text("SELECT * FROM users WHERE email = :email"),
            {"email": email}
        ).fetchone()
        
        if user:
            return {
                "already_user": True,
                "queue_position": user._mapping.get("queue_position"),
                "message": "You already have an account. Please login."
            }
        
        # Check if already in waitlist (not converted)
        existing = conn.execute(
            text("SELECT * FROM waitlist WHERE email = :email AND converted_to_user = FALSE"),
            {"email": email}
        ).fetchone()
        
        if existing:
            return {
                "id": existing._mapping["id"],
                "email": existing._mapping["email"],
                "queue_position": existing._mapping["queue_position"],
                "already_in_waitlist": True
            }
        
        # Get next queue position (combine users + waitlist for accurate position)
        max_user_pos = conn.execute(
            text("SELECT COALESCE(MAX(queue_position), 0) FROM users")
        ).scalar()
        max_waitlist_pos = conn.execute(
            text("SELECT COALESCE(MAX(queue_position), 0) FROM waitlist")
        ).scalar()
        next_position = max(max_user_pos or 0, max_waitlist_pos or 0) + 1
        
        # Insert new waitlist entry
        result = conn.execute(
            text("""
                INSERT INTO waitlist (email, full_name, queue_position)
                VALUES (:email, :full_name, :queue_position)
                RETURNING id, email, queue_position, created_at
            """),
            {"email": email, "full_name": full_name, "queue_position": next_position}
        )
        conn.commit()
        
        row = result.fetchone()
        return {
            "id": row._mapping["id"],
            "email": row._mapping["email"],
            "queue_position": row._mapping["queue_position"],
            "created_at": str(row._mapping["created_at"]),
            "success": True
        }


def get_waitlist_entry(*, engine: Engine, email: str) -> dict | None:
    """Get waitlist entry by email."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM waitlist WHERE email = :email"),
            {"email": email}
        ).fetchone()
        
        if result:
            return dict(result._mapping)
        return None


def get_total_waitlist_count(*, engine: Engine) -> int:
    """Get total count of waitlist entries."""
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM waitlist WHERE converted_to_user = FALSE")
        ).scalar()
        return count or 0


def convert_to_user(*, engine: Engine, email: str, hashed_password: str, full_name: str = None) -> dict | None:
    """
    Convert a waitlist entry to a full user.
    Returns the created user or None if not in waitlist.
    """
    with engine.connect() as conn:
        # Get waitlist entry
        waitlist_entry = conn.execute(
            text("SELECT * FROM waitlist WHERE email = :email AND converted_to_user = FALSE"),
            {"email": email}
        ).fetchone()
        
        if not waitlist_entry:
            return None
        
        queue_position = waitlist_entry._mapping["queue_position"]
        wl_full_name = waitlist_entry._mapping.get("full_name") or full_name
        
        # Create user with waitlist queue position
        result = conn.execute(
            text("""
                INSERT INTO users (email, hashed_password, full_name, queue_position, approval_status)
                VALUES (:email, :hashed_password, :full_name, :queue_position, 'pending')
                RETURNING id, email, full_name, queue_position, approval_status, created_at
            """),
            {
                "email": email,
                "hashed_password": hashed_password,
                "full_name": wl_full_name,
                "queue_position": queue_position
            }
        )
        
        user_row = result.fetchone()
        
        # Mark waitlist entry as converted
        conn.execute(
            text("""
                UPDATE waitlist 
                SET converted_to_user = TRUE, converted_at = NOW()
                WHERE email = :email
            """),
            {"email": email}
        )
        
        conn.commit()
        
        return {
            "id": user_row._mapping["id"],
            "email": user_row._mapping["email"],
            "full_name": user_row._mapping["full_name"],
            "queue_position": user_row._mapping["queue_position"],
            "approval_status": user_row._mapping["approval_status"],
            "converted_from_waitlist": True
        }

