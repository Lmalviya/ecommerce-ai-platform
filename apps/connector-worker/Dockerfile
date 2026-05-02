FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy the entire workspace to ensure local packages resolve correctly
COPY . .

# Sync dependencies for the workspace
RUN uv sync --all-packages

# Set the working directory to the specific app
WORKDIR /app/apps/connector-worker

# Run celery worker
CMD ["uv", "run", "celery", "-A", "src.celery_app", "worker", "--loglevel=info", "--concurrency=2"]
