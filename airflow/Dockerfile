# Use official Spark image with Java pre-installed
FROM bitnami/spark:3.5.1

# Switch to root to install Python packages
USER root

# Install Python3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Airflow, Delta Lake, and other dependencies
RUN pip3 install apache-airflow==2.7.1 \
                 delta-spark==2.4.0 \
                 pandas \
                 numpy

# Copy your DAGs into Airflow folder
COPY dags/ /opt/airflow/dags/

# Set working directory
WORKDIR /opt/airflow

# Expose Airflow webserver port
EXPOSE 8080

# Start Airflow standalone
CMD ["airflow", "standalone"]
