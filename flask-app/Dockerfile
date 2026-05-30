FROM ghcr.io/astral-sh/uv:debian-slim

# Install system dependencies needed for meshcore and its C-extensions (like bleak/dbus-fast)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     libdbus-1-dev \
#     libglib2.0-dev \
#     pkg-config \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy application
COPY . .

# Install system dependencies
RUN uv sync --frozen

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV UV_COMPILE_BYTECODE=1

EXPOSE 5050
CMD ["uv", "run", "python", "app.py"]   