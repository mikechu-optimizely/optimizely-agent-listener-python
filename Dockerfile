# Use the official Python 3.13 slim image as base
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install uv
RUN pip install --no-cache-dir uv

# Copy requirements file
COPY requirements.txt .

# Install dependencies using uv
RUN uv pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY main.py google_analytics.py amplitude.py ./

# Run the application
CMD ["python", "main.py"]
