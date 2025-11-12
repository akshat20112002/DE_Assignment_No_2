FROM ubuntu:18.04

# Install Python 3.8 and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.8 python3-pip python3-dev \
        libpq-dev build-essential libssl-dev libffi-dev \
        curl vim software-properties-common && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and core packaging tools
RUN python3.8 -m pip install --upgrade pip setuptools wheel

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install requirements (pytest + polars-lts-cpu)
RUN pip3 install --no-cache-dir -r requirements.txt

# Verify installs
RUN python3.8 -m pip show polars-lts-cpu && python3.8 -m pip show pytest

# Default command
CMD ["python3.8", "main.py"]
