FROM ghcr.io/astral-sh/uv:alpine
WORKDIR /app

# Install build dependencies for C-extensions on Alpine
RUN apk add --no-cache gcc musl-dev python3-dev libffi-dev dbus-dev glib-dev

# Copy application
COPY . .

# Install system dependencies
RUN uv sync --frozen

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=development
# ENV FLASK_ENV=production

EXPOSE 5050
CMD ["uv", "run", "python", "app.py"]   