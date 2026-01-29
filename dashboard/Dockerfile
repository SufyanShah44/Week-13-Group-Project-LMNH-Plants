FROM python:3.12

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY components.py .

COPY dashboard.py .

CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]