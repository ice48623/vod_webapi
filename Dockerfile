FROM python:3.7-alpine

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

RUN chmod +x ./main.py

CMD ["./main.py"]