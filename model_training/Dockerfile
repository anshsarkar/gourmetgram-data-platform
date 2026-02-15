# Base Image: Lightweight Python 3.11
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for psycopg2 (Postgres driver)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the notebooks and scripts
COPY training.ipynb .
COPY training_large_data_demo.ipynb .
COPY generate_large_dataset.py .

# Create a directory for model artifacts
RUN mkdir models

# Expose Jupyter port
EXPOSE 8888

# Command to run Jupyter Lab
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
