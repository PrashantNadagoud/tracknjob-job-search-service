from fastapi import Request
from jose import JWTError, jwt

from app.config import get_settings

ALGORITHM = "RS256"


async def get_current_user(request: Request) -> dict:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise _UnauthorizedError("Missing or malformed Authorization header")

    token = auth_header[len("Bearer "):]
    settings = get_settings()

    try:
        payload = jwt.decode(
            token,
            settings.TNJ_JWT_PUBLIC_KEY,
            algorithms=[ALGORITHM],
        )
    except JWTError:
        raise _UnauthorizedError("Invalid or expired token")

    sub = payload.get("sub")
    email = payload.get("email")
    if not sub or not email:
        raise _UnauthorizedError("Token missing required claims")

    return {"sub": str(sub), "email": str(email)}


class _UnauthorizedError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)
