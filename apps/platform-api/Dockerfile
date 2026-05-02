FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy the entire workspace to ensure local packages resolve correctly
COPY . .

# Sync dependencies for the workspace
RUN uv sync --all-packages

# Set the working directory to the specific app
WORKDIR /app/apps/platform-api

# Expose port 8000
EXPOSE 8000

# Run uvicorn server using the uv virtual environment
CMD ["uv", "run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
