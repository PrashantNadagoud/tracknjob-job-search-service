from fastapi import Request
from jose import JWTError, jwt

from app.config import get_settings

ALGORITHM = "HS256"


async def get_current_user(request: Request) -> dict:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise _UnauthorizedError("Missing or malformed Authorization header")

    token = auth_header[len("Bearer "):]
    settings = get_settings()

    if not settings.TNJ_SECRET_KEY:
        raise _UnauthorizedError("Server misconfiguration: secret key not set")

    try:
        payload = jwt.decode(
            token,
            settings.TNJ_SECRET_KEY,
            algorithms=[ALGORITHM],
        )
    except JWTError:
        raise _UnauthorizedError("Invalid or expired token")

    sub = payload.get("sub")
    if not sub:
        raise _UnauthorizedError("Token missing subject claim")

    return {"sub": str(sub), "email": str(payload.get("email", ""))}


class _UnauthorizedError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)
