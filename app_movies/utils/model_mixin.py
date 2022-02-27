from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse

from config.settings import MOVIES_PAGE_PAGINATE_BY
from movies.models import FilmWork, PersonRole


class MoviesApiMixin:
    model = FilmWork
    http_method_names = ['get']
    paginate_by = MOVIES_PAGE_PAGINATE_BY

    @staticmethod
    def __person_in_role(role):
        return ArrayAgg(
            'personfilmwork__person_id__full_name',
            distinct=True,
            filter=Q(personfilmwork__role=role)
        )

    def get_queryset(self):
        queryset = super().get_queryset()
        queryset = queryset.prefetch_related('genres', 'persons')
        queryset = queryset.values(
            'id',
            'title',
            'description',
            'creation_date',
            'rating',
            'type'
        )
        queryset = queryset.annotate(
            genres=ArrayAgg('genrefilmwork__genre_id__name', distinct=True),
            actors=self.__person_in_role(PersonRole.ACTOR),
            directors=self.__person_in_role(PersonRole.DIRECTOR),
            writers=self.__person_in_role(PersonRole.PRODUCER),
        )
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)
