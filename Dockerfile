FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./
COPY config.yaml ./

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Data volume for SQLite database
VOLUME ["/app/data"]

# Set environment variables
ENV COPY_TRADER_DB_PATH=/app/data/trades.db
ENV PYTHONUNBUFFERED=1

# Expose health check port
EXPOSE 8080

# Default command - uses environment variables for config
ENTRYPOINT ["python", "copy_trader.py"]
CMD ["run"]
