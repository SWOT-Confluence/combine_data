# Stage 0 - Create from Python3.9.5 image
# FROM python:3.9-slim-buster as stage0
FROM python:3.9-slim-buster

# Stage 1 - Copy and execute module
# FROM stage0 as stage1
COPY ./combine_data.py /app/combine_data.py
LABEL version="1.0" \
        description="Containerized combine_data module." \
        "confluence.contact"="ntebaldi@umass.edu" \
        "algorithm.contact"="ntebaldi@umass.edu"
ENTRYPOINT ["/usr/local/bin/python3", "/app/combine_data.py"]