FROM prefecthq/prefect:3-python3.13

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt --proxy http://10.255.249.100:3128
COPY . .
