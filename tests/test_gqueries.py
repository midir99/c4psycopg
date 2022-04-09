import pytest

from c4psycopg.queries import (columns_with_phs, commas, csidentifiers,
                               csplaceholders, delete_by_cpk, delete_by_pk,
                               delete_many_by_cpk, delete_many_by_pk, insert,
                               insert_many, ob, rows_with_phs,
                               rows_with_phs_and_defaults, select_by_cpk,
                               select_by_pk, select_many_by_cpk,
                               select_many_by_pk)


@pytest.mark.parametrize(
    "obs,expected",
    [
        (
            (ob("username"), ob("email", "ASC"), ob("name", nulls="LAST")),
            '"username","email" ASC,"name" NULLS LAST',
        ),
        (
            (ob("username", "DESC"), ob("email")),
            '"username" DESC,"email"',
        ),
    ],
)
def test_commas(postgresql, obs, expected):
    result = commas(*obs).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "rows,phs,expected",
    [
        (3, 5, "(%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s),(%s,%s,%s,%s,%s)"),
        (1, 1, "(%s)"),
        (4, 2, "(%s,%s),(%s,%s),(%s,%s),(%s,%s)"),
    ],
)
def test_rows_with_phs(postgresql, rows, phs, expected):
    result = rows_with_phs(rows=rows, phs=phs).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "columns,defaults_per_entity,expected",
    [
        (
            ("customerid", "name", "email"),
            (
                ("customerid",),
                ("email", "customerid"),
                (),
            ),
            "(DEFAULT,%s,%s),(DEFAULT,%s,DEFAULT),(%s,%s,%s)",
        ),
        (
            ("userid", "username", "age"),
            (
                ("userid", "username", "age"),
                (),
                ("1", "2"),
                ("age",),
            ),
            "(DEFAULT,DEFAULT,DEFAULT),(%s,%s,%s),(%s,%s,%s),(%s,%s,DEFAULT)",
        ),
    ],
)
def test_rows_with_phs_and_defaults(postgresql, columns, defaults_per_entity, expected):
    result = rows_with_phs_and_defaults(columns, defaults_per_entity).as_string(
        postgresql
    )
    assert result == expected


@pytest.mark.parametrize(
    "columns,named_phs,expected",
    [
        (
            ("username", "email", "first_name"),
            False,
            '"username"=%s,"email"=%s,"first_name"=%s',
        ),
        (
            ("username", "email", "first_name"),
            True,
            '"username"=%(username)s,"email"=%(email)s,"first_name"=%(first_name)s',
        ),
    ],
)
def test_columns_with_phs(postgresql, columns, named_phs, expected):
    result = columns_with_phs(columns, named_phs=named_phs).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "columns,defaults,named_phs,expected",
    [
        (
            ("username", "email"),
            None,
            True,
            "%(username)s,%(email)s",
        ),
        (
            ("username", "email"),
            None,
            False,
            "%s,%s",
        ),
        (
            ("username", "email"),
            ("email",),
            True,
            "%(username)s,DEFAULT",
        ),
    ],
)
def test_csplaceholders(postgresql, columns, defaults, named_phs, expected):
    result = csplaceholders(columns, defaults=defaults, named_phs=named_phs).as_string(
        postgresql
    )
    assert result == expected


@pytest.mark.parametrize(
    "columns,expected",
    [
        (("username", "email", "password"), '"username","email","password"'),
        (("username", "email"), '"username","email"'),
    ],
)
def test_csidentifiers(postgresql, columns, expected):
    result = csidentifiers(columns).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "column,order,nulls,expected",
    [
        ("name", None, None, '"name"'),
        ("email", "ASC", None, '"email" ASC'),
        ("username", "DESC", "FIRST", '"username" DESC NULLS FIRST'),
        ("last_name", None, "LAST", '"last_name" NULLS LAST'),
    ],
)
def test_ob(postgresql, column, order, nulls, expected):
    result = ob(column, order=order, nulls=nulls).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "table,columns,defaults,returning,named_phs,expected",
    [
        (
            "customer",
            ("customerid", "name", "email"),
            None,
            False,
            True,
            'INSERT INTO "customer" ("customerid","name","email") VALUES '
            "(%(customerid)s,%(name)s,%(email)s)",
        ),
        (
            "customer",
            ("customerid", "name", "email"),
            None,
            False,
            False,
            'INSERT INTO "customer" ("customerid","name","email") VALUES (%s,%s,%s)',
        ),
        (
            "customer",
            ("customerid", "name", "email"),
            None,
            True,
            False,
            'INSERT INTO "customer" ("customerid","name","email") VALUES (%s,%s,%s) '
            'RETURNING "customerid","name","email"',
        ),
        (
            "customer",
            ("customerid", "name", "email"),
            ("customerid",),
            True,
            False,
            'INSERT INTO "customer" ("customerid","name","email") VALUES (DEFAULT,%s,%s) '
            'RETURNING "customerid","name","email"',
        ),
    ],
)
def test_insert(postgresql, table, columns, defaults, returning, named_phs, expected):
    result = insert(
        table, columns, defaults=defaults, returning=returning, named_phs=named_phs
    ).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "table,columns,qty,defaults_per_entity,returning,expected",
    [
        (
            "customer",
            ("customerid", "name", "email"),
            3,
            None,
            False,
            'INSERT INTO "customer" ("customerid","name","email") VALUES '
            "(%s,%s,%s),(%s,%s,%s),(%s,%s,%s)",
        ),
        (
            "customer",
            ("customerid", "name", "email"),
            2,
            None,
            True,
            'INSERT INTO "customer" ("customerid","name","email") VALUES '
            '(%s,%s,%s),(%s,%s,%s) RETURNING "customerid","name","email"',
        ),
        (
            "customer",
            ("customerid", "name", "email"),
            1,
            (
                ("customerid",),
                ("email", "customerid"),
                (),
            ),
            True,
            'INSERT INTO "customer" ("customerid","name","email") VALUES '
            "(DEFAULT,%s,%s),(DEFAULT,%s,DEFAULT),(%s,%s,%s) RETURNING "
            '"customerid","name","email"',
        ),
    ],
)
def test_insert_many(
    postgresql, table, columns, qty, defaults_per_entity, returning, expected
):
    result = insert_many(
        table,
        columns,
        qty=qty,
        defaults_per_entity=defaults_per_entity,
        returning=returning,
    ).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "table,pk_column,columns,named_phs,expected",
    [
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            True,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            "%(customerid)s",
        ),
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            False,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            "%s",
        ),
    ],
)
def test_select_by_pk(postgresql, table, pk_column, columns, named_phs, expected):
    result = select_by_pk(table, pk_column, columns, named_phs=named_phs).as_string(
        postgresql
    )
    assert result == expected


@pytest.mark.parametrize(
    "table,pk_column,columns,order_by,limit,offset,named_phs,expected",
    [
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            None,
            None,
            None,
            True,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            "ANY(%(customerid)s)",
        ),
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            None,
            None,
            None,
            False,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            "ANY(%s)",
        ),
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            commas(ob("name"), ob("email", "ASC")),
            None,
            None,
            False,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            'ANY(%s) ORDER BY "name","email" ASC',
        ),
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            commas(ob("name"), ob("email", "ASC")),
            100,
            None,
            False,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            'ANY(%s) ORDER BY "name","email" ASC LIMIT 100',
        ),
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            commas(ob("name"), ob("email", "ASC")),
            100,
            500,
            False,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            'ANY(%s) ORDER BY "name","email" ASC LIMIT 100 OFFSET 500',
        ),
        (
            "customer",
            "customerid",
            ("customerid", "name", "email"),
            None,
            None,
            500,
            False,
            'SELECT "customerid","name","email" FROM "customer" WHERE "customerid" = '
            "ANY(%s) OFFSET 500",
        ),
    ],
)
def test_select_many_by_pk(
    postgresql,
    table,
    pk_column,
    columns,
    order_by,
    limit,
    offset,
    named_phs,
    expected,
):
    result = select_many_by_pk(
        table,
        pk_column,
        columns,
        order_by=order_by,
        limit=limit,
        offset=offset,
        named_phs=named_phs,
    ).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "table,pk_columns,columns,named_phs,expected",
    [
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            True,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") = '
            "(%(quarter_id)s,%(course_id)s,%(student_id)s)",
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            False,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") = (%s,%s,%s)',
        ),
    ],
)
def test_select_by_cpk(postgresql, table, pk_columns, columns, named_phs, expected):
    result = select_by_cpk(table, pk_columns, columns, named_phs=named_phs).as_string(
        postgresql
    )
    assert result == expected


@pytest.mark.parametrize(
    "table,pk_columns,columns,qty,order_by,limit,offset,expected",
    [
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            2,
            None,
            None,
            None,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") IN ((%s,%s,%s),(%s,%s,%s))',
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            1,
            ob("grades", "DESC"),
            None,
            None,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") IN ((%s,%s,%s)) ORDER BY '
            '"grades" DESC',
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            2,
            ob("grades", "DESC"),
            4,
            None,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") IN ((%s,%s,%s),(%s,%s,%s)) '
            'ORDER BY "grades" DESC LIMIT 4',
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            1,
            ob("grades", "DESC"),
            4,
            100,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") IN ((%s,%s,%s)) '
            'ORDER BY "grades" DESC LIMIT 4 OFFSET 100',
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            ("quarter_id", "course_id", "student_id", "grades"),
            1,
            None,
            None,
            100,
            'SELECT "quarter_id","course_id","student_id","grades" FROM "course_grades"'
            ' WHERE ("quarter_id","course_id","student_id") IN ((%s,%s,%s)) OFFSET 100',
        ),
    ],
)
def test_select_many_by_cpk(
    postgresql,
    table,
    pk_columns,
    columns,
    qty,
    order_by,
    limit,
    offset,
    expected,
):
    result = select_many_by_cpk(
        table,
        pk_columns,
        columns,
        qty=qty,
        order_by=order_by,
        limit=limit,
        offset=offset,
    ).as_string(postgresql)
    assert result == expected


@pytest.mark.parametrize(
    "table,pk_column,returning,columns,named_phs,expected,exception",
    [
        (
            "customer",
            "customerid",
            False,
            None,
            False,
            'DELETE FROM "customer" WHERE "customerid" = %s',
            False,
        ),
        (
            "customer",
            "customerid",
            True,
            ("customerid", "name", "email"),
            False,
            'DELETE FROM "customer" WHERE "customerid" = %s RETURNING '
            '"customerid","name","email"',
            False,
        ),
        (
            "customer",
            "customerid",
            True,
            ("customerid", "name", "email"),
            True,
            'DELETE FROM "customer" WHERE "customerid" = %(customerid)s RETURNING '
            '"customerid","name","email"',
            False,
        ),
        (
            "customer",
            "customerid",
            True,
            None,
            True,
            'DELETE FROM "customer" WHERE "customerid" = %(customerid)s RETURNING '
            '"customerid","name","email"',
            True,
        ),
    ],
)
def test_delete_by_pk(
    postgresql,
    table,
    pk_column,
    returning,
    columns,
    named_phs,
    expected,
    exception,
):
    if exception:
        with pytest.raises(
            ValueError, match="If returning is True, columns must be specified."
        ):
            delete_by_pk(
                table,
                pk_column,
                returning=returning,
                columns=columns,
                named_phs=named_phs,
            )
    else:
        result = delete_by_pk(
            table,
            pk_column,
            returning=returning,
            columns=columns,
            named_phs=named_phs,
        ).as_string(postgresql)
        assert result == expected


@pytest.mark.parametrize(
    "table,pk_column,returning,columns,named_phs,expected,exception",
    [
        (
            "customer",
            "customerid",
            False,
            None,
            False,
            'DELETE FROM "customer" WHERE "customerid" = ANY(%s)',
            False,
        ),
        (
            "customer",
            "customerid",
            True,
            ("customerid", "name", "email"),
            False,
            'DELETE FROM "customer" WHERE "customerid" = ANY(%s) RETURNING '
            '"customerid","name","email"',
            False,
        ),
        (
            "customer",
            "customerid",
            True,
            ("customerid", "name", "email"),
            True,
            'DELETE FROM "customer" WHERE "customerid" = ANY(%(customerid)s) RETURNING '
            '"customerid","name","email"',
            False,
        ),
        (
            "customer",
            "customerid",
            True,
            None,
            True,
            'DELETE FROM "customer" WHERE "customerid" = ANY(%(customerid)s) RETURNING '
            '"customerid","name","email"',
            True,
        ),
    ],
)
def test_delete_many_by_pk(
    postgresql,
    table,
    pk_column,
    returning,
    columns,
    named_phs,
    expected,
    exception,
):
    if exception:
        with pytest.raises(
            ValueError, match="If returning is True, columns must be specified."
        ):
            delete_many_by_pk(
                table,
                pk_column,
                returning=returning,
                columns=columns,
                named_phs=named_phs,
            )
    else:
        result = delete_many_by_pk(
            table,
            pk_column,
            returning=returning,
            columns=columns,
            named_phs=named_phs,
        ).as_string(postgresql)
        assert result == expected


@pytest.mark.parametrize(
    "table,pk_columns,returning,columns,named_phs,expected,exception",
    [
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            False,
            None,
            False,
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") = '
            "(%s,%s,%s)",
            False,
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            True,
            ("quarter_id", "course_id", "student_id", "grades"),
            False,
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") = '
            '(%s,%s,%s) RETURNING "quarter_id","course_id","student_id","grades"',
            False,
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            True,
            ("quarter_id", "course_id", "student_id", "grades"),
            True,
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") = '
            "(%(quarter_id)s,%(course_id)s,%(student_id)s) RETURNING "
            '"quarter_id","course_id","student_id","grades"',
            False,
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            True,
            None,
            True,
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") = '
            "(%(quarter_id)s,%(course_id)s,%(student_id)s) RETURNING "
            '"quarter_id","course_id","student_id","grades"',
            True,
        ),
    ],
)
def test_delete_by_cpk(
    postgresql,
    table,
    pk_columns,
    returning,
    columns,
    named_phs,
    expected,
    exception,
):
    if exception:
        with pytest.raises(
            ValueError, match="If returning is True, columns must be specified."
        ):
            delete_by_cpk(
                table,
                pk_columns,
                returning=returning,
                columns=columns,
                named_phs=named_phs,
            )
    else:
        result = delete_by_cpk(
            table,
            pk_columns,
            returning=returning,
            columns=columns,
            named_phs=named_phs,
        ).as_string(postgresql)
        assert result == expected


@pytest.mark.parametrize(
    "table,pk_columns,qty,returning,columns,expected,exception",
    [
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            2,
            False,
            None,
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") '
            "IN ((%s,%s,%s),(%s,%s,%s))",
            False,
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            3,
            True,
            ("quarter_id", "course_id", "student_id", "grades"),
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") '
            "IN ((%s,%s,%s),(%s,%s,%s),(%s,%s,%s)) RETURNING "
            '"quarter_id","course_id","student_id","grades"',
            False,
        ),
        (
            "course_grades",
            ("quarter_id", "course_id", "student_id"),
            3,
            True,
            None,
            'DELETE FROM "course_grades" WHERE ("quarter_id","course_id","student_id") '
            "IN ((%s,%s,%s),(%s,%s,%s),(%s,%s,%s)) RETURNING "
            '"quarter_id","course_id","student_id","grades"',
            True,
        ),
    ],
)
def test_delete_many_by_cpk(
    postgresql,
    table,
    pk_columns,
    qty,
    returning,
    columns,
    expected,
    exception,
):
    if exception:
        with pytest.raises(
            ValueError, match="If returning is True, columns must be specified."
        ):
            delete_many_by_cpk(
                table,
                pk_columns,
                qty=qty,
                returning=returning,
                columns=columns,
            )
    else:
        result = delete_many_by_cpk(
            table,
            pk_columns,
            qty=qty,
            returning=returning,
            columns=columns,
        ).as_string(postgresql)
        assert result == expected
