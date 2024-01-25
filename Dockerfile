# Stage 0 - Create from Python3.9.5 image
# FROM python:3.9-slim-buster as stage0
FROM python:3.9-slim-buster

# Stage 1 - Create virtual environment and install dependencies
# FROM stage0 as stage1
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 2 - Copy and execute module
# FROM stage1 as stage2
COPY ./combine_data.py /app/combine_data.py

LABEL version="1.0" \
      description="Containerized combine_data module."
ENTRYPOINT ["/app/env/bin/python3", "/app/combine_data.py"]