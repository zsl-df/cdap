FROM python:2.7-alpine
WORKDIR /automation/
COPY . /automation/
RUN apk add alpine-sdk cyrus-sasl-dev wget sudo curl postgresql-dev libffi-dev
RUN pip install --extra-index-url http://192.168.192.201:5050/simple/ --trusted-host 192.168.192.201 -r requirements.txt