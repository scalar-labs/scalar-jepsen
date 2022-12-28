FROM ubuntu:20.04
ENV LEIN_ROOT=true
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -y -q update && \
    apt-get install -y \
        openjdk-11-jdk \
        libjna-java \
        git \
        gnuplot \
        wget \
        graphviz \
        vim

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

ADD ./init.sh /init.sh
RUN chmod +x /init.sh
ENTRYPOINT ["sh", "/init.sh"]
