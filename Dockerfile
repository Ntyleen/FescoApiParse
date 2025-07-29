FROM python:3.10-slim-bookworm

WORKDIR /app

RUN useradd -m appuser \
    && chown -R appuser:appuser /app \
    && mkdir -p /var/log/FescoApiParser \
    && chown -R appuser:appuser /var/log/FescoApiParser

RUN apt update && apt install -y libfbclient2

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV TZ=Asia/Vladivostok
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENTRYPOINT [ "python", "main.py", "schedule"]
