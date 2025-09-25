FROM python:3-slim
WORKDIR /programas/ingesta
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
CMD [ "python3", "./ingesta.py" ]
