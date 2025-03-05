  FROM python:3.10-slim

  WORKDIR /app

  # Copy requirements file
  COPY requirements.txt .

  # Debug commands
  RUN echo "Python version:" && python --version && \
      echo "Pip version:" && pip --version && \
      echo "Directory contents:" && ls -la && \
      echo "Requirements file contents:" && cat requirements.txt && \
      echo "Installing streamlit only:" && \
      pip install streamlit && \
      echo "Streamlit installation successful!"

  # The CMD instruction needs to be a single command - this was the issue
  CMD ["python", "-c", "import time; print('Container is running...'); time.sleep(3600)"]
