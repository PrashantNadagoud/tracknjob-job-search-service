## 2026-05-21 - [CRITICAL] Admin Endpoint Lacked Authentication
**Vulnerability:** The `/seed-status` endpoint in `artifacts/job-search-api/app/api/v1/admin.py` was exposed without any authentication, allowing unauthenticated users to access sensitive database aggregate counts and seed pipeline metrics.
**Learning:** The `admin.py` router was mistakenly assumed to be an internal-only endpoint and wasn't wrapped with the `Depends(get_current_user)` check that is standard across other API routers.
**Prevention:** All API endpoints, especially those under an `/admin` prefix or returning internal system state, must explicitly enforce authentication (`get_current_user`) and role-based access control (checking against `ADMIN_USER_ID`) regardless of their intended network exposure.
