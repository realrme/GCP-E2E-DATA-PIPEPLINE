FROM python:3.10-slim
RUN pip install pandas sqlalchemy "cloud-sql-python-connector[pg8000]"
COPY data/grocery_chain_data.json /data/grocery_chain_data.json
COPY seed_cloud_sql.py /seed_cloud_sql.py
CMD ["python", "/seed_cloud_sql.py"]
