#!/usr/bin/env python3

"""
This script bootstraps a PostgreSQL DB with data from the O'Reilly API endpoint.
"""

__author__ = "Bradley Frank"

import json
import os
import sys
import urllib.request
from urllib.error import HTTPError
from urllib.error import URLError

import logzero
import psycopg2
from logzero import logger
from psycopg2 import sql
from psycopg2.extras import execute_batch

# Set this to False to quiesce logzero debugging.
DEBUG = True

# See https://www.oreilly.com/online-learning/integration-docs/search.html.
API_URL = "https://learning.oreilly.com/api/v2/search/"
LIMIT = 200
TOPIC = "python"
FIELDS = ["isbn", "authors", "title", "description"]

# Database information is passed from environment.
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DB_HOST = "postgres"
DB_NAME = os.environ["POSTGRES_USER"]

# Table definitions for saving API query results.
DB_TABLES = {
    "books": [
        "book_id serial NOT NULL PRIMARY KEY",
        "title text NOT NULL",
        "isbn bigint",
        "description text",
    ],
    "authors": [
        "author_id serial NOT NULL PRIMARY KEY",
        "name text NOT NULL UNIQUE",
    ],
    "books_authors": [
        "book_id int REFERENCES books (book_id)",
        "author_id int REFERENCES authors (author_id)",
        "CONSTRAINT books_authors_pkey PRIMARY KEY (book_id, author_id)",
    ],
}


def db_connect():
    """Connects to a PostgreSQL DB using credentials from Docker environment variables."""

    logger.debug("Host: %s; Name: %s; User: %s; Pass: %s", DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)

    try:
        conn = psycopg2.connect(
            database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port="5432"
        )
    except (Exception, psycopg2.DatabaseError) as err:
        print("Error connecting to PostgreSQL.")
        logger.error(err)
        sys.exit()

    return conn


def create_tables(conn):
    """Drops existing tables and creates new DB scaffolding."""

    pg_exec(conn, "DROP TABLE IF EXISTS " + ", ".join(DB_TABLES.keys()), "Error dropping tables.")

    # Programatically create tables by looping through DB_TABLES dictionary.
    for table, rows in DB_TABLES.items():
        query = sql.SQL("CREATE TABLE {pg_table} " + "( " + ", ".join(rows) + " )").format(
            pg_table=sql.Identifier(table),
        )

        pg_exec(conn, query, "Error creating tables.")


def query_api():
    """Queries the O'Reilly API endpoint for works based on topic."""

    get = (
        API_URL + "?query=" + TOPIC + "&limit=" + str(LIMIT) + "&fields=" + "&fields=".join(FIELDS)
    )
    logger.debug(get)

    try:
        response = urllib.request.urlopen(get)
    except HTTPError as err:
        print("There was an HTTP error.")
        logger.error(err)
        sys.exit()
    except URLError as err:
        print("Could not connect to endpoint.")
        logger.error(err)
        sys.exit()

    encoding = response.info().get_content_charset("utf-8")
    feed = json.loads(response.read().decode(encoding))

    return feed["results"]


def pg_exec(conn, postgres_query, msg, **kwargs):
    """Wrapper function for performing PostgreSQL queries and optionally returning values."""

    # Optional arguments that change how SQL is executed and if it returns results.
    fetch = kwargs.get("fetch", None)
    data = kwargs.get("data", None)
    batch = kwargs.get("batch", None)

    #
    # psycopg2 requires data to be in the form list of tuples:
    # example = [(field1,), (field2,), ... (fieldN,),]
    #

    #
    # Batch mode:
    #   Psycopg will join the statements into fewer multi-statement commands, each one containing
    #   at most page_size statements, resulting in a reduced number of server roundtrips.
    # https://www.psycopg.org/docs/extras.html?highlight=batch#psycopg2.extras.execute_batch
    #

    try:
        cursor = conn.cursor()
        logger.debug(postgres_query)
        if batch:
            execute_batch(cursor, postgres_query, data)
        else:
            if data:
                cursor.execute(postgres_query, data)
            else:
                cursor.execute(postgres_query)
    except (Exception, psycopg2.DatabaseError) as err:
        print(msg)
        logger.error(err)
        cursor.close()
        conn.close()
        sys.exit()
    else:
        if fetch:
            result = cursor.fetchone()
        else:
            result = None
        conn.commit()
        cursor.close()

    return result


def dump_authors(conn, works):
    """Dumps all authors, uniquely, from API query into the PostreSQL DB."""

    list_of_authors = []

    # Creates a unique list of authors to insert. Generates a list.
    for entry in works:
        for author in entry["authors"]:
            if author not in list_of_authors:
                list_of_authors.append(author)

    query_ins_authors = sql.SQL("INSERT INTO {pg_table} (name) VALUES (%s)").format(
        pg_table=sql.Identifier("authors"),
    )

    # Lambda for converting list to list of tuples that psycopg2 expects.
    pg_exec(
        conn,
        query_ins_authors,
        "Error inserting into 'authors'.",
        fetch=False,
        data=list(map(lambda a: tuple([a]), list_of_authors)),
        batch=True,
    )


def dump_books(conn, works):
    """Dumps book metadata into PostgreSQL, querying author table for relationship data."""

    #
    # The table 'books_authors' gives the relationship between books and their authors. For
    # each book, find the author(s) in the 'author' table and create the relationship.
    #

    # SQL to insert data into the 'books' table.
    query_ins_books = sql.SQL(
        "INSERT INTO {pg_table} ({pg_fields}) VALUES (%s, %s, %s) RETURNING book_id;"
    ).format(
        pg_table=sql.Identifier("books"),
        pg_fields=sql.SQL(',').join([
            sql.Identifier('title'),
            sql.Identifier('isbn'),
            sql.Identifier('description'),
        ]),
    )

    # SQL to select data from the 'authors' table.
    query_sel_author = sql.SQL("SELECT {pg_fields} FROM {pg_table} WHERE name LIKE %s").format(
        pg_fields=sql.Identifier("author_id"), pg_table=sql.Identifier("authors"),
    )

    # SQL to insert data into the 'books_authors' table.
    query_ins_books_author = sql.SQL(
        "INSERT INTO {pg_table} ({pg_fields}) VALUES (%s, %s);"
    ).format(
        pg_table=sql.Identifier("books_authors"),
        pg_fields=sql.SQL(",").join([sql.Identifier("book_id"), sql.Identifier("author_id")]),
    )

    for entry in works:
        title = entry["title"]
        isbn = entry["isbn"] if "isbn" in entry else "0"
        description = entry["description"]

        #
        # Insert the book into 'books' table and use the book_id to associate the book
        # with author(s) in the 'authors' table.
        #
        book_id = pg_exec(
            conn,
            query_ins_books,
            "Error inserting into 'books'.",
            fetch=True,
            data=[(title,), (isbn,), (description,),],
        )

        logger.debug("Title: %s; ID: %s", title, book_id)

        for author in entry["authors"]:
            #
            # Find the author(s) in the 'authors' table, and use the author_id to associate the
            # author with a book_id in the 'books' table.
            #
            author_id = pg_exec(
                conn,
                query_sel_author,
                "Error selecting from 'authors'.",
                fetch=True,
                data=[(author,),],
            )

            logger.debug("Author: %s; ID: %s", author, author_id)

            pg_exec(
                conn,
                query_ins_books_author,
                "Error inserting into 'books_authors'.",
                data=[(book_id,), (author_id,),],
            )


if DEBUG:
    logzero.loglevel()
else:
    logzero.loglevel(0)

pg_conn = db_connect()
create_tables(pg_conn)
oreilly_works = query_api()
dump_authors(pg_conn, oreilly_works)
dump_books(pg_conn, oreilly_works)
pg_conn.close()
