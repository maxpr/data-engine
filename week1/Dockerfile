FROM python:3.9.1

RUN apt-get install wget

WORKDIR /app

COPY requirements.txt requirements.txt
COPY src /app/src/

RUN pip install -r requirements.txt

RUN ["chmod", "+x","/app/src/ingest.sh"]
RUN ["chmod", "+x","/app/src/run_jupyter.sh"]


CMD /app/src/run_jupyter.sh