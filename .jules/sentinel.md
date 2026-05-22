## 2024-05-22 - Unauthenticated Admin API Endpoint
**Vulnerability:** The internal admin API endpoint `/api/v1/admin/seed-status` was exposed without authentication, leaking sensitive system statistics and internal configuration.
**Learning:** The router was explicitly initialized without dependencies (`APIRouter()`) despite exposing internal data.
**Prevention:** Always enforce authorization checks (e.g. `Depends(admin_required)`) on APIRouters handling internal or administrative functionality.
