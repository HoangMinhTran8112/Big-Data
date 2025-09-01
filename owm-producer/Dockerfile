FROM python:3.8

WORKDIR /usr/app

# Download and install necessary packages
ADD ./requirements.txt ./
RUN pip install -r requirements.txt
ADD ./ ./

CMD ["python3", "-u", "openweathermap_producer.py"]
