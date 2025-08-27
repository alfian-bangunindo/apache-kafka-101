FROM python:3.12.7-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Install system dependencies for PySpark, Postgres, etc.
RUN apt-get update && apt-get install -y \
	openjdk-17-jdk \
	build-essential \
	libpq-dev \
	curl \
	wget \
	&& rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY . .

CMD ["python", "main.py"]

