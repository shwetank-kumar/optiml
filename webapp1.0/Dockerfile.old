# app/Dockerfile

FROM python:3.9.13

WORKDIR /webapp


RUN git clone https://github.com/shwetank-kumar/optiml.git

RUN pip install -r requirements.txt

EXPOSE 8501


ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]