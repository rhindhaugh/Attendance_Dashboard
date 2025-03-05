# Attendance Dashboard

A dashboard for analyzing employee attendance data, providing insights on office attendance patterns.

## Features

- Visualize attendance trends over time
- Filter by date range and employee attributes
- Analyze core day attendance patterns
- Generate reports on attendance metrics

## Local Development

### Prerequisites

- Python 3.12+
- Pandas, Streamlit, and other dependencies in `requirements.txt`

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd Attendance_Dashboard
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Place input data in the `data/raw` directory:
   - `key_card_access.csv`: Key card access records
   - `employee_info.csv`: Employee information
   - `employment_status_history.csv`: Employment status history (optional)

5. Run the dashboard:
   ```bash
   streamlit run src/dashboard.py
   ```

### Using the CLI

The project includes a command-line interface for easier execution:

```bash
./run_dashboard.sh --days 30 --memory-opt
```

For a list of available options:
```bash
./run_dashboard.sh --help
```

## Deployment

### Docker

Build and run the Docker image:

```bash
docker build -t attendance-dashboard:latest .
docker run -p 8501:8501 -v $(pwd)/data:/app/data attendance-dashboard:latest
```

### Kubernetes

Deploy to Kubernetes using Helm:

```bash
helm install attendance-dashboard ./k8s/helm/attendance-dashboard \
  --set image.repository=your-registry-url.com/attendance-dashboard \
  --set ingress.hosts[0].host=attendance-dashboard.your-domain.com
```

## Configuration

The application uses a centralized configuration system in `src/config.py` for:
- Data locations
- Analysis parameters
- Special employee IDs

## Testing

Run tests using:

```bash
python -m unittest discover -s tests
```
