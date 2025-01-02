FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies for the app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Default command for running tests
#CMD ["pytest", "tests"]
CMD ["airflow", "webserver"]
