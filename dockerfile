# 1. Use an official, lightweight Python base image
FROM python:3.11-slim

# 2. Install uv directly into the container from the official binaries
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 3. Set the working directory inside the container's filesystem
WORKDIR /app

# 4. Copy your dependency files first (enables Docker caching for fast builds)
COPY pyproject.toml uv.lock ./

# 5. Install your project's dependencies using uv
# --system tells uv to install packages directly into the container's global Python environment
RUN uv pip install --system -r pyproject.toml

# 6. Copy your source code into the container
# COPY src/ ./src/      # This is no longer needed because we are mounting the src directory as a volume in the Airflow DAG

# 7. Define the environment variable placeholders (Good documentation for the container)
ENV GCP_PROJECT_ID=""
ENV GCS_BUCKET_NAME=""

# 8. The command that executes automatically when the container boots up
CMD ["python", "src/extract.py"]