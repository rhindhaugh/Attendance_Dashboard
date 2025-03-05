 FROM python:3.10-slim

  WORKDIR /app

  # Copy just requirements for debugging
  COPY requirements.txt .

  # Debug commands
  RUN echo "Python version:" && python --version && \
      echo "Pip version:" && pip --version && \
      echo "Directory contents:" && ls -la && \
      echo "Requirements file contents:" && cat requirements.txt && \
      echo "Installing streamlit only:" && \
      pip install streamlit && \
      echo "Streamlit installation successful!"

  # Just keep the container running for testing
  CMD ["python", "-c", "import time; print('Container is running...'); 
  time.sleep(3600)"]
