import uuid

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(TimeStampedMixin, UUIDMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class FilmType(models.TextChoices):
    MOVIE = 'movie', _('movie')
    TV_SERIES = 'TV series', _('series')


class FilmWork(TimeStampedMixin, UUIDMixin):
    title = models.TextField(_('title'), db_index=True)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(
        _('creation date'),
        blank=True,
        db_index=True,
    )
    certificate = models.CharField(
        _('certificate'),
        max_length=512,
        blank=True,
        null=True,
    )
    file_path = models.FileField(
        _('file'),
        blank=True,
        null=True,
        upload_to='movies/',
    )
    rating = models.FloatField(
        _('rating'), blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )
    type = models.CharField(_('type'), choices=FilmType.choices, max_length=32)
    genres = models.ManyToManyField(
        Genre,
        through='GenreFilmWork',
        verbose_name=_('genres'),
    )
    persons = models.ManyToManyField('Person', through='PersonFilmWork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('film production')
        verbose_name_plural = _('film productions')

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    genre = models.ForeignKey(
        'Genre',
        on_delete=models.CASCADE,
        verbose_name=_('genre'),
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')
        constraints = [
            models.UniqueConstraint(
                fields=['film_work_id', 'genre_id'],
                name='unique_film_work_genre_idx',
            )
        ]


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.TextField(_('full name'), db_index=True)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')

    def __str__(self):
        return self.full_name


class PersonRole(models.TextChoices):
    ACTOR = 'actor', _('actor')
    PRODUCER = 'producer', _('producer')
    DIRECTOR = 'director', _('director')


class PersonFilmWork(UUIDMixin):
    film_work = models.ForeignKey('FilmWork', on_delete=models.CASCADE)
    person = models.ForeignKey(
        'Person',
        on_delete=models.CASCADE,
        verbose_name=_('person'),
        related_name='person_fw'
    )
    role = models.TextField(_('role'), choices=PersonRole.choices, null=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('person')
        verbose_name_plural = _('persons')
        constraints = [
            models.UniqueConstraint(
                fields=['film_work_id', 'person', 'role'],
                name='unique_film_work_person_role_idx',
            )
        ]