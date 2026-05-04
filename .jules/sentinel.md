## 2024-05-28 - Missing Authentication on Admin Endpoints
**Vulnerability:** The `/api/v1/admin/seed-status` endpoint has no authentication required.
**Learning:** In the `artifacts/job-search-api/app/api/v1/admin.py` file, the endpoint is completely open.
**Prevention:** Make sure `Depends(get_current_user)` is applied to all admin endpoints.
