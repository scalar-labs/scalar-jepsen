FROM ubuntu:24.04

RUN apt-get -y -q update && \
    apt-get install -y \
    sudo \
    build-essential \
    openssh-server \
    psmisc \
    python3 \
    ntpdate \
    gnupg \
    iproute2 \
    iptables

ADD ./init.sh /init.sh
RUN chmod +x /init.sh
ENTRYPOINT ["sh", "/init.sh"]
