import psycopg.rows

from c4psycopg import queries
from c4psycopg.ems import EntityManager


class TestEntityManager:
    def test___init__(self):
        table = "customer"
        pk = "customerid"
        columns = ("email", "first_name", "last_name", "age")
        defaults = {
            "email": lambda: "No email.",
        }
        em = EntityManager(table, pk, columns, defaults=defaults)
        assert em.table == table
        assert em.pk == pk
        assert em.columns == (pk,) + columns
        assert em.row_factory is psycopg.rows.dict_row
        assert "insert" in em.cached_queries
        assert em.cached_queries["insert"] == queries.insert(
            em.table, em.columns, returning=False, named_phs=True
        )
        assert "rinsert" in em.cached_queries
        assert em.cached_queries["rinsert"] == queries.insert(
            em.table,
            em.columns,
            returning=True,
            named_phs=True,
        )
        assert "select_by_pk" in em.cached_queries
        assert em.cached_queries["select_by_pk"] == queries.select_by_pk(
            em.table,
            em.pk,
            em.columns,
            named_phs=False,
        )
        assert "delete_by_pk" in em.cached_queries
        assert em.cached_queries["delete_by_pk"] == queries.delete_by_pk(
            em.table,
            em.pk,
            returning=False,
            named_phs=False,
        )
        assert "rdelete_by_pk" in em.cached_queries
        assert em.cached_queries["rdelete_by_pk"] == queries.delete_by_pk(
            em.table,
            em.pk,
            returning=True,
            columns=em.columns,
            named_phs=False,
        )
        assert "delete_many_by_pk" in em.cached_queries
        assert em.cached_queries["delete_many_by_pk"] == queries.delete_many_by_pk(
            em.table,
            em.pk,
            returning=False,
            named_phs=False,
        )
        assert "rdelete_many_by_pk" in em.cached_queries
        assert em.cached_queries["rdelete_many_by_pk"] == queries.delete_many_by_pk(
            em.table,
            em.pk,
            returning=True,
            columns=em.columns,
            named_phs=False,
        )

    def test_add_defaults(self):
        ...

    def test_create(self):
        ...

    def test_create_many(self):
        ...
