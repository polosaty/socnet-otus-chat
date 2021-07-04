FROM python:3.9-alpine3.13
LABEL maintainer="Aksarin Mikhail <m.aksarin@gmail.com>"

ARG UID=1000

WORKDIR /app

ARG TZ=Asia/Yekaterinburg

ENV LANG=ru_RU.UTF-8 \
    LANGUAGE=ru_RU.UTF-8 \
    LC_CTYPE=ru_RU.UTF-8 \
    LC_ALL=ru_RU.UTF-8 \
    TERM=xterm

RUN apk add --update --no-cache tzdata

RUN ln -fs /usr/share/zoneinfo/$TZ /etc/localtime

ENV WAIT_VERSION 2.7.2
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait /wait
RUN chmod +x /wait

ADD ./requirements.txt /tmp/

RUN apk add --virtual .build-deps --no-cache --update \
    cmake make musl-dev gcc g++ gettext-dev libintl git \
    python3-dev libffi-dev openssl-dev cargo && \
    git clone https://github.com/rilian-la-te/musl-locales.git && \
    cd musl-locales && cmake . && make && make install && \
    rm -rf musl-locales && \
    pip3 install -r /tmp/requirements.txt && \
    apk del .build-deps && \
    adduser \
        --disabled-password \
        --no-create-home \
        --shell /bin/bash \
        --gecos "" \
        --uid ${UID} \
        --home /app \
    app && \
    chown -R app:app /app

ADD . /app

USER app
