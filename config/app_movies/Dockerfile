FROM python:3.9.6-alpine
WORKDIR /var/www/move_admin
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apk --no-cache add \
    icu-dev \
    gettext \
    gettext-dev

RUN pip install --upgrade pip
COPY ./app_movies/requirements.txt .
RUN pip install -r requirements.txt
COPY ./config/app_movies/entrypoint.sh /usr/local/bin
COPY ./config/app_movies/fixtures.json /tmp
EXPOSE 8000