FROM apache/airflow:2.5.1-python3.10

# Install the required Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple blpapi \
    pdblp==0.1.8

# Copy Airflow DAGs and other necessary files
COPY ./src/dags /opt/airflow/dags
COPY ./src/api /opt/airflow/api
COPY ./src/helpers /opt/airflow/helpers
COPY ./src/utils /opt/airflow/utils

# Set the working directory
WORKDIR /opt/airflow

# Ensure the entrypoint is still set to start Airflow
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
