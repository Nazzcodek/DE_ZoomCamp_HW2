FROM prefecthq/prefect:2.7.7-python3.9

COPY docker_requirements.txt .

RUN pip install -r docker_requirements.txt --trusted-host pypi.python.org --no-cache-dir

RUN mkdir -p /opt/prefect/data/
RUN mkdir -p /opt/prefect/data/yellow
RUN mkdir -p /opt/prefect/flows/

COPY flows /opt/prefect/flows