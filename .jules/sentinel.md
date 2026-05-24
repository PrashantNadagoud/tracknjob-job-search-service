## 2024-05-23 - [Critical] Unauthenticated Admin Endpoints Exposed
**Vulnerability:** The `/seed-status` admin endpoint was completely unauthenticated and returned sensitive backend crawl status and aggregate metrics.
**Learning:** A route can easily be exposed if an explicit `dependencies=[Depends(admin_required)]` is missed during route setup, even if the filename or module is named `admin`.
**Prevention:** Apply authentication middleware globally or have a security linter catch endpoints without security dependencies. Always explicitly protect internal-use administrative endpoints.
