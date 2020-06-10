FROM ubuntu:20.04

# tzdata wants to throw up an interactive message requesting input,so just specify your timezone here instead and it wont.
ENV TZ 'Australia/Perth'
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get -y update

RUN apt-get -y install git \
                       python3-pip \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN git clone "https://github.com/ICRAR/manta-ray-client" \
    && cd manta-ray-client \
    && pip3 install -r requirements.txt \
    && python3 setup.py install 

ENTRYPOINT /bin/bash
