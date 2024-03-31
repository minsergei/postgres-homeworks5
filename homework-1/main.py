"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2
import os


def open_csv(way):
    path_csv = os.path.abspath(way)
    with open(path_csv) as file:
        list_data = []
        next(file)
        filereader = csv.reader(file)
        for elem in filereader:
            list_data.append(elem)
        return list_data


def add_north(way, sql):
    with psycopg2.connect(host='localhost', database='north', user='postgres', password='12345') as conn:
        with conn.cursor() as cur:
            data_csv = open_csv(way)
            cur.executemany(sql, data_csv)
    conn.close()


add_north('north_data/customers_data.csv', "INSERT INTO customers VALUES (%s, %s, %s)")
add_north('north_data/employees_data.csv', "INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)")
add_north('north_data/orders_data.csv', "INSERT INTO orders_data VALUES (%s, %s, %s, %s, %s)")
