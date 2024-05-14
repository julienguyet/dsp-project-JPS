FROM apache/airflow:2.9.0
COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

RUN pip install streamlit

FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

# 
#RUN pip install --upgrade pip
#RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
#COPY ./fastapi_app /code/fastapi_app

# 
#CMD ["fastapi", "run", "fastapi_app/main.py", "--port", "8000"]