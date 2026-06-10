FROM python:3.11-slim
WORKDIR /app
COPY requirement.txt .
RUN apt-get update && apt-get install -y python3-pip
RUN pip install -r requirement.txt
