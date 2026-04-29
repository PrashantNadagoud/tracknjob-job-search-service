## 2025-02-14 - Unauthenticated Admin Endpoints
**Vulnerability:** The `/api/v1/admin/seed-status` endpoint in the `job-search-api` was completely unauthenticated and didn't require admin privileges.
**Learning:** This exposes potentially sensitive infrastructure information (e.g. crawler state, pipeline data). It reveals an architectural pattern where internal endpoints grouped under an `admin` path didn't inherently inherit admin checks unless explicitly added.
**Prevention:** Always explicitly define dependencies for authentication/authorization even for "internal" looking paths. We fixed this by adding `Depends(get_current_user)` and an admin sub claim verification step.
