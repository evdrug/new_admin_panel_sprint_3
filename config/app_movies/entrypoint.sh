#!/bin/sh

while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do sleep 1; done;

python manage.py migrate movies 0001 --fake
python manage.py migrate
python manage.py createcachetable
python manage.py compilemessages -l en -l ru
python manage.py collectstatic  --noinput
if [ "$DJANGO_SUPERUSER_USERNAME" ]
then
    python manage.py createsuperuser \
        --noinput \
        --username $DJANGO_SUPERUSER_USERNAME \
        --email $DJANGO_SUPERUSER_EMAIL || true
fi
python manage.py loaddata /tmp/fixtures.json

gunicorn config.wsgi:application --bind 0.0.0.0:8000 --reload

exec "$@"