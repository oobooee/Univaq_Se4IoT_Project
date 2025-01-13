FROM python:3.11-slim
# Imposta la directory di lavoro nel container
WORKDIR /app

# Copia i file necessari nel container
COPY . ./

RUN pip install --trusted-host pypi.python.org -r lib.txt

CMD ["python", "-u", "hydro_dam.py"]
