from dbt.adapters.polars.catalogs.databricksCatalog import (
    alter_column_comment_sql,
    comment_on_table_sql,
    quote_identifier,
    quoted_full_name,
)


def test_quote_identifier_wraps_in_backticks():
    assert quote_identifier("my_table") == "`my_table`"


def test_quote_identifier_escapes_embedded_backticks():
    assert quote_identifier("weird`name") == "`weird``name`"


def test_quoted_full_name_quotes_each_part():
    assert quoted_full_name("cat", "schema", "table") == "`cat`.`schema`.`table`"


def test_comment_on_table_sql_reserved_word_column_table_name():
    # Table names can be reserved words too - the whole qualified name must
    # be quoted, not just the leaf identifier.
    name = quoted_full_name("cat", "schema", "order")
    sql = comment_on_table_sql(name, "a comment")
    assert sql == "COMMENT ON TABLE `cat`.`schema`.`order` IS 'a comment'"


def test_comment_on_table_sql_escapes_single_quotes():
    name = quoted_full_name("cat", "schema", "table")
    sql = comment_on_table_sql(name, "it's a table")
    assert "it''s a table" in sql
    assert "it's a table" not in sql


def test_alter_column_comment_sql_quotes_reserved_word_column():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comment_sql(name, "date", "when it happened")
    assert sql == (
        "ALTER TABLE `cat`.`schema`.`table` ALTER COLUMN `date` "
        "COMMENT 'when it happened'"
    )


def test_alter_column_comment_sql_quotes_another_reserved_word_column():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comment_sql(name, "order", "the order value")
    assert "ALTER COLUMN `order`" in sql


def test_alter_column_comment_sql_escapes_single_quotes_in_comment():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comment_sql(name, "col", "user's note")
    assert "user''s note" in sql
    assert "user's note" not in sql


def test_alter_column_comment_sql_escapes_backtick_in_column_name():
    name = quoted_full_name("cat", "schema", "table")
    sql = alter_column_comment_sql(name, "weird`col", "a comment")
    assert "`weird``col`" in sql
