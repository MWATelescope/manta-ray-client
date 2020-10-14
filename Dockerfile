FROM python:3.8

RUN apt-get -y update \
    && apt-get -y install git \
                  python3-pip \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && apt-get autoremove \
    && apt-get clean

COPY . /manta-ray-client

RUN cd manta-ray-client \
    && pip3 install -r requirements.txt \
    && python3 setup.py install 

ENTRYPOINT /bin/bash
