# Use a slim Python image
FROM python:3.12-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# 1. Copy workspace configuration
COPY pyproject.toml uv.lock ./

# 2. Copy the local package and the app
COPY packages/py-storage ./packages/py-storage
COPY apps/dev-tools ./apps/dev-tools

# 3. Install dependencies 
# (Removed --frozen so it can fix the lockfile mismatch)
RUN uv sync --no-install-project

# 4. Set Python Path
ENV PYTHONPATH=/app/apps/dev-tools/src:/app/packages/py-storage

# Expose FastAPI port
EXPOSE 8000

# 5. Start the application
# We use 'python -m uvicorn' which is more reliable inside containers
CMD ["uv", "run", "--package", "dev-tools", "python", "-m", "uvicorn", "main:app", "--app-dir", "apps/dev-tools/src", "--host", "0.0.0.0", "--port", "8000"]
