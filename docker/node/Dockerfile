FROM ubuntu:20.04

RUN apt-get -y -q update && \
    apt-get install -y \
    sudo \
    openssh-server \
    psmisc \
    python \
    ntpdate \
    iproute2 \
    iptables

ADD ./init.sh /init.sh
RUN chmod +x /init.sh
ENTRYPOINT ["sh", "/init.sh"]
