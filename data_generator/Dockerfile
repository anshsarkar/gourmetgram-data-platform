FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire data_generator package into /app/data_generator
COPY . /app/data_generator/

# Run the data generator as a module
CMD ["python", "-m", "data_generator.main"]
