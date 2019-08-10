FROM ubuntu:latest

# Install Python 3.7, 2.7 is standard to ubuntu:latest
RUN apt-get update && \
    apt-get install -y python3.7 && \
    apt-get install -y python3-pip

RUN pip3 install pytest

# Set workspace
RUN mkdir /berserkr-app
WORKDIR /berserkr-app

# Python
ENV alias python=/usr/bin/python3.6
ENV PYTHONPATH /berserkr-app/

# Add the project files to the
COPY src ./src
COPY scripts ./scripts
COPY tests ./tests
COPY tools ./tools

# Make run.sh executable
RUN chmod +x /berserkr-app/scripts/run.sh

ENTRYPOINT ["./scripts/run.sh"]

# Replace the Entrypoint running the run.sh with this
# to keep the container alive, to be able to debug the container
#ENTRYPOINT ["tail", "-f", "/dev/null"]