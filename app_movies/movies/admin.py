from django.contrib import admin

from .models import Genre, FilmWork, GenreFilmWork, Person, PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ('name', 'description')


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    extra = 0
    autocomplete_fields = ('genre',)



class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    extra = 0
    # fk_name = 'film_work'
    fields = ('person', 'role',)
    raw_id_fields = ('person',)
    ordering = ('person__full_name',)


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline,)
    list_display = ('title', 'type', 'creation_date', 'rating',)
    list_filter = ('type', 'genres')
    search_fields = ('title', 'description', 'id')

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.prefetch_related('persons', 'genres')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ('full_name',)
