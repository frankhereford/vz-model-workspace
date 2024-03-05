FROM python:3.12-bullseye

RUN apt-get update && apt-get install -y aptitude wormhole vim black curl lsb-release gnupg2

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg

RUN apt-get update && apt-get install -y postgresql-client-16

RUN mkdir /application
WORKDIR /application

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

CMD [ "/bin/bash"]
