FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "exec gunicorn phanta_app:app --bind 0.0.0.0:${PORT:-8080}"]
