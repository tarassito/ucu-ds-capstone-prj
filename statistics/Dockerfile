FROM python:3.8-slim

WORKDIR /app
RUN mkdir -p /app/logs

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "faust", "-A", "app", "worker"]
