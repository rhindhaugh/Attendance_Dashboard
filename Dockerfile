FROM python:3.12-slim

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies with detailed debugging
RUN pip install --upgrade pip && \
    pip list && \
    echo "Contents of requirements.txt:" && \
    cat requirements.txt && \
    echo "Attempting to install packages..." && \
    pip install --no-cache-dir -r requirements.txt || { echo "INSTALLATION FAILED"; pip install --no-cache-dir --verbose -r requirements.txt; exit 1; }

# Copy the rest of the application
COPY . .

# Create directories for data if they don't exist
RUN mkdir -p data/raw data/processed logs

# Expose the port Streamlit will run on
EXPOSE 8501

# Set up environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ENABLE_CORS=false

# Command to run the application
ENTRYPOINT ["streamlit", "run", "src/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
