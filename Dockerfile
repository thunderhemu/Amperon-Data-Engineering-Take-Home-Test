# Use the pinned image for immutability (Senior Practice: Deterministic Builds)
FROM python:3.11.7@sha256:63bec515ae23ef6b4563d29e547e81c15d80bf41eff5969cb43d034d333b63b8

# 1. Environment Variables
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# 2. Setup Virtual Environment and Install Dependencies
WORKDIR /app
COPY requirements.txt .

# Create a non-global venv for isolation and install dependencies
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Update PATH to use the VENV bin directory
ENV PATH="/opt/venv/bin:$PATH"

# 3. Copy Application Code and Configuration
COPY tomorrow ./tomorrow
COPY config ./config
COPY scripts ./scripts
COPY analysis.ipynb .
# --> ADD THIS LINE: Copy the tests directory
COPY tests ./tests

# 4. Create Non-Root User (Senior Practice: Security/Principle of Least Privilege)
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# The application runs via 'python -m tomorrow.scheduler' defined in docker-compose.