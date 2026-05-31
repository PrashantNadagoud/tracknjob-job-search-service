#!/bin/sh
echo "=== Container starting ==="
echo "PORT=${PORT}"
echo "DATABASE_URL prefix=$(echo $DATABASE_URL | cut -c1-30)..."
echo ""

echo "=== Running Alembic migrations (120s timeout) ==="
timeout 120 alembic upgrade head
ALEMBIC_EXIT=$?
if [ $ALEMBIC_EXIT -eq 124 ]; then
    echo "=== Alembic timed out after 120s — starting server without migrations ==="
elif [ $ALEMBIC_EXIT -ne 0 ]; then
    echo "=== Alembic exited with code $ALEMBIC_EXIT — starting server anyway ==="
else
    echo "=== Alembic completed successfully ==="
fi

echo ""
echo "=== Starting Uvicorn on port ${PORT} ==="
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
