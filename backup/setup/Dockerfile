FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jdk curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pyspark==3.5.3 delta-spark==3.2.1 notebook mlflow requests

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app
COPY . .

EXPOSE 8888

CMD ["jupyter", "notebook","--ip=0.0.0.0","--port=8888","--no-browser","--allow-root","--NotebookApp.token=","--NotebookApp.password="]