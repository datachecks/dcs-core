FROM python:3.9
RUN apt-get update && apt-get install -y \
    python3-dev libpq-dev wget unzip \
    python3-setuptools gcc bc
RUN pip install --no-cache-dir poetry==1.1.13
COPY . /app
WORKDIR /app
RUN poetry install
ENTRYPOINT ["poetry", "python3", "run", "-m" , "datachecks"]
