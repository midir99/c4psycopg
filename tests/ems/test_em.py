import pytest
import psycopg.rows

from c4psycopg import queries
from c4psycopg.ems import EntityManager


class TestEntityManager:
    def test___init__(self):
        table = "customer"
        pk = "customerid"
        columns = ("email", "first_name", "last_name", "age")
        defaults = {
            "email": lambda: "No email",
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
        table = "customer"
        pk = "customerid"
        columns = ("customerid", "email", "first_name", "last_name", "age")
        with pytest.raises(
            ValueError, match="The pk must not be included in the columns."
        ):
            EntityManager(table, pk, columns)

    def test_add_defaults(self):
        table = "customer"
        pk = "customerid"
        columns = ("email", "first_name", "last_name", "age")
        defaults = {
            "email": lambda: "Unknown email",
            "age": lambda: -1,
        }
        em = EntityManager(table, pk, columns, defaults)
        entity = {
            "first_name": "Blanca",
            "last_name": "Roman",
        }
        em.add_defaults(entity)
        assert entity == {
            "first_name": "Blanca",
            "last_name": "Roman",
            "email": "Unknown email",
            "age": -1,
        }

    def test_create(self):
        ...

    def test_create_many(self):
        ...

    def test_find_by_pk(self, cpostgresql):
        table = "customer"
        pk = "customerid"
        columns = ("email", "first_name", "last_name", "age")
        em = EntityManager(table, pk, columns)

        customer1 = em.find_by_pk(1, cpostgresql)
        assert customer1 == {
            "customerid": 1,
            "email": "amaya.hudson8772@google.net",
            "first_name": "Amaya",
            "last_name": "Hudson",
            "age": 66,
        }

        customer404 = em.find_by_pk(404, cpostgresql)
        assert customer404 is None

    def test_find_many_pk(self, cpostgresql):
        table = "customer"
        pk = "customerid"
        columns = ("email", "first_name", "last_name", "age")
        pk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        em = EntityManager(table, pk, columns)

        customers = em.find_many_by_pk(pk_list, cpostgresql)
        assert len(customers) == 10

        limit = 5
        customers = em.find_many_by_pk(pk_list, cpostgresql, limit=limit)
        assert len(customers) == 5

        offset = 5
        customers = em.find_many_by_pk(pk_list, cpostgresql, offset=offset)
        assert len(customers) == 5
        assert customers[0]["customerid"] == 6
        assert customers[1]["customerid"] == 7
        assert customers[2]["customerid"] == 8
        assert customers[3]["customerid"] == 9
        assert customers[4]["customerid"] == 10

        limit = 3
        offset = 4
        customers = em.find_many_by_pk(pk_list, cpostgresql, limit=limit, offset=offset)
        assert len(customers) == 3
        assert customers[0]["customerid"] == 5
        assert customers[1]["customerid"] == 6
        assert customers[2]["customerid"] == 7

        order_by = queries.ob("customerid", "DESC")
        customers = em.find_many_by_pk(pk_list, cpostgresql, order_by=order_by)
        assert len(customers) == 10
        assert customers[0]["customerid"] == 10
        assert customers[1]["customerid"] == 9
        assert customers[2]["customerid"] == 8
        assert customers[3]["customerid"] == 7
        assert customers[4]["customerid"] == 6
        assert customers[5]["customerid"] == 5
        assert customers[6]["customerid"] == 4
        assert customers[7]["customerid"] == 3
        assert customers[8]["customerid"] == 2
        assert customers[9]["customerid"] == 1
