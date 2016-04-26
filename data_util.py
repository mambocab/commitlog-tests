#!/usr/bin/env python
"""
USAGE: {__file__} generate [--output-file OUTPUT] --keyspace-name KS_NAME --table-name TABLE_NAME
       {__file__} load DATAFILE --keyspace-name KS_NAME --table-name TABLE_NAME
       {__file__} validate DATAFILE --keyspace-name KS_NAME --table-name TABLE_NAME

OPTIONS:
    --output-file OUTPUT -o OUTPUT  Optional output file. stderr by default.
"""
from __future__ import print_function

import csv
from StringIO import StringIO
import sys
from random import randint

from cassandra.cluster import Cluster
# from cassandra.concurrent import execute_concurrent_with_args
from docopt import docopt


def csv_handle_to_nested_list(fh):
    return [x for x in
            list(list(map(int, row)) for row in fh)
            if x]

def generate(output_file, ks_name, table_name):
    session = Cluster().connect()

    def echo_and_exec(stmt):
        print(stmt, file=sys.stderr)
        session.execute(stmt)

    echo_and_exec(
        "CREATE KEYSPACE {ks} WITH replication = ".format(ks=ks_name) +
        "{'class': 'SimpleStrategy' , 'replication_factor': 1 };"
    )

    header = ['foo', 'bar', 'baz', 'quux']
    echo_and_exec(
        "CREATE TABLE {ks}.{tab} ({ints_from_header}"
        "PRIMARY KEY ({pk_from_header}));".format(
            ks=ks_name,
            tab=table_name,
            ints_from_header=' int,'.join(header) + ' int, ',
            pk_from_header=', '.join(header)
        )
    )

    builder = StringIO()
    writer = csv.writer(builder)
    # write header
    writer.writerow(header)

    data = [
        [randint(-1000, 1000) for _ in header]
        for _ in range(50000)
    ]

    for row in data:
        writer.writerow(row)
    print(builder.getvalue(), file=output_file)


def load(datafile, ks_name, table_name):
    with open(datafile) as f:
        reader = csv.reader(f)
        session = Cluster().connect()

        header = reader.next()

        stmt = (
            "INSERT INTO {ks}.{tab} ({header_spec}) VALUES ({qs});"
        ).format(ks=ks_name,
                 tab=table_name,
                 header_spec=','.join(header),
                 qs=','.join(['?' for _ in header]))
        # print('preparing {stmt}'.format(stmt=stmt), file=sys.stderr)
        prepared = session.prepare(stmt)
        data = csv_handle_to_nested_list(reader)
        # print(data, file=sys.stderr)

        for row in data:
            session.execute(prepared, row)

        # execute_concurrent_with_args(
        #     session=session,
        #     statement=prepared,
        #     parameters=data,
        #     concurrency=2
        # )


def validate(datafile, ks_name, table_name):
    with open(datafile) as f:
        reader = csv.reader(f)
        session = Cluster().connect()

        # consume the header
        reader.next()

        from_csv = csv_handle_to_nested_list(reader)
        from_cassandra = [
            list(row) for row in
            session.execute('SELECT * FROM {ks}.{tab};'.format(ks=ks_name, tab=table_name))
        ]

        # print("here's what we got from the csv:\n{from_csv}".format(from_csv=from_csv))
        # print("here's what we got from cassandra:\n{from_cassandra}".format(from_cassandra=from_cassandra))
        assert sorted(from_csv) == sorted(from_cassandra), (
            'uh-oh!\n'
            '{from_csv}\n'
            '{from_cassandra}\n'
        ).format(from_csv=from_csv, from_cassandra=from_cassandra)


if __name__ == '__main__':
    opts = docopt(__doc__.format(__file__=__file__))
    print(opts, file=sys.stderr)
    ks_name, table_name = opts['KS_NAME'], opts['TABLE_NAME']
    datafile = opts['DATAFILE']
    output_file = opts['--output-file'] or sys.stdout
    if opts['generate']:
        generate(output_file=output_file, ks_name=ks_name, table_name=table_name)
    elif opts['load']:
        load(datafile=datafile, ks_name=ks_name, table_name=table_name)
    elif opts['validate']:
        validate(datafile=datafile, ks_name=ks_name, table_name=table_name)
