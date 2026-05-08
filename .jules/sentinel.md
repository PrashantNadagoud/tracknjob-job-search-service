## 2024-05-08 - [CORS Middleware Restriction]
**Vulnerability:** The API server used a generic `app.use(cors())` which enabled Cross-Origin Resource Sharing for all origins. This could allow malicious websites to make requests to the API.
**Learning:** Even in internal or mock environments, configuring CORS strictly by default provides defense in depth. Environment variables like `FRONTEND_URL` should be leveraged.
**Prevention:** Always restrict CORS explicitly instead of using the parameterless default `cors()` configuration.
