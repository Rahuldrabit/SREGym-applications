FROM python:3.11-slim

WORKDIR /app

COPY app.py .

EXPOSE 8002

ENV PYTHONUNBUFFERED=1
ENV PORT=8002

CMD ["python", "app.py"]
