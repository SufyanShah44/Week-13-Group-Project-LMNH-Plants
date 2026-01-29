FROM python:latest

RUN apt-get update && apt-get install -y curl unixodbc odbcinst
RUN curl -sSL -O https://packages.microsoft.com/config/debian/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1)/packages-microsoft-prod.deb
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY components.py .
COPY queries_s3.py .

COPY dashboard_merge.py .

CMD ["streamlit", "run", "dashboard_merge.py", "--server.address=0.0.0.0", "--server.port=8501"]