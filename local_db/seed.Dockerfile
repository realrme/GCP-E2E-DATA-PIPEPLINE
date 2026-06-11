FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary python-dotenv

COPY data/grocery_chain_data.json /app/local_db/data/grocery_chain_data.json
COPY seed_db.py /app/local_db/seed_db.py

CMD ["python", "/app/local_db/seed_db.py"]
