FROM python:3.6 

RUN apt-get -y update \
    && apt-get -y install git \
                  python3-pip \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && apt-get autoremove \
    && apt-get clean

RUN git clone "https://github.com/ICRAR/manta-ray-client" \
    && cd manta-ray-client \
    && pip3 install -r requirements.txt \
    && python3 setup.py install 

ENTRYPOINT /bin/bash
