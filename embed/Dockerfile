FROM python:3.9

WORKDIR /app

COPY . /app

RUN pip install pika
RUN pip install owl2vec_star
RUN pip install numpy

ENV PYTHONUNBUFFERED=1


CMD ["python3", "/app/worker.py"]