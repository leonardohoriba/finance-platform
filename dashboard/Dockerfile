FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DATABASE_USER=postgres
ENV DATABASE_PASSWORD=postgres
ENV DATABASE_DB=datalake
ENV DATABASE_HOST=postgres

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless", "true"]
