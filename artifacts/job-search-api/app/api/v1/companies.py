"""Company intelligence endpoint."""
import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.db import get_db
from app.models import Company
from app.schemas.companies import CompanyResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/{slug}",
    response_model=CompanyResponse,
    summary="Get company intelligence by slug",
)
async def get_company(
    slug: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
) -> CompanyResponse:
    try:
        stmt = select(Company).where(Company.slug == slug)
        row = (await db.execute(stmt)).scalar_one_or_none()
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database query failed")

    if row is None:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse.model_validate(row)
