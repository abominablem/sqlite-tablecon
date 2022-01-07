# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 22:29:57 2021

@author: marcu
"""

import sqlite3 as sql

class TableCon:
    def __init__(self, db, table = None, debug = False):
        """
        isolation_level = None causes the database to operate in autocommit
        mode. This means transactions are committed as soon as they are
        issued, removing "database is locked" errors.
        """
        if db[-3:] != ".db":
            db += ".db"
        self.con = sql.connect(db, isolation_level = None)
        self.cur = self.con.cursor()
        self.table = table
        self.debug = debug
        self.field_map = {}

    def open(self, db, table):
        if not self.con is None:
            self.close()

        self.set_db(db)
        self.set_table(table)

    def close(self):
        if not self.con is None:
            self.con.close()
            self.con = None
            self.cur = None

    def commit(self):
        if not self.con is None:
            self.con.commit()

    def set_db(self, db):
        self.close()
        self.con = sql.connect(db + ".db")
        self.cur = self.con.cursor()

    def set_table(self, table):
        self.table = table

    def get_columns(self):
        cols = self.execute("PRAGMA table_info(%s)" % self.table,
                            select = True)
        col_dict = {x[1]: {'order': x[0], 'type': x[2],
                           'nullable': (x[3] == 1), 'default': x[4]}
                    for x in cols}
        return col_dict

    def get_tables(self):
        return self.execute("SELECT name FROM sqlite_master "
                            "WHERE type='table'", select = True)

    def define_field_map(self, field_map):
        """
        Define a map from field names used in write_values to those in the
        specified table. Persistent across changes in table or database.
        """
        self.field_map = field_map

    def map_field_names(self, fields):
        if isinstance(fields, dict):
            mapped_dict = {}
            for k, v in fields.items():
                mapped_dict[self.field_map[k] if k in self.field_map
                            else k] = v
            return mapped_dict
        elif isinstance(fields, list):
            return [self.field_map[k] if k in self.field_map else k
                    for k in fields]
        elif isinstance(fields, str):
            return (self.field_map[fields] if fields in self.field_map
                    else fields)

    def add_row(self, **kwargs):
        """
        Given either a dictionary of field names mapped to field values, or
        field names as keywords with values field values, build the INSERT INTO
        SQL string.
        """
        kwargs = self.map_field_names(kwargs)
        sql_col = "INSERT INTO %s (" % self.table
        sql_val = "VALUES ("
        for k, v in kwargs.items():
            v = self._sanitise(v)
            sql_col += self._bracket(k) + ","
            sql_val += self._quote(v) + ","

        sql_col = sql_col[:-1]
        sql_val = sql_val[:-1]

        query = sql_col + ") " + sql_val + ")"
        self.execute(query, select = False)

    def _quote(self, string):
        return "'%s'" % string

    def _bracket(self, string):
        return "[%s]" % string

    def _sanitise(self, value):
        if isinstance(value, str):
            return value.replace("'", "''")
        elif isinstance(value, list):
            return [self._sanitise(v) for v in value]
        else:
            return value

    def execute(self, query, select = False):
        # if self.table is None and not "CREATE TABLE" in query:
        #     raise AttributeError("No table defined")

        if self.debug: print(query)

        cursor = self.cur.execute(query)

        if select:
            return cursor.fetchall()
        else:
            self.commit()

    def select(self, query):
        return self.execute(query, select = True)

    def filter(self, filters, return_cols, rc = "columns", boolean = "AND",
               distinct = True):
        """
        filters is a dictionary of column names and values to filter with.
        return_cols is a list of column names to return
        """
        if isinstance(return_cols, str):
            return_cols = [return_cols]

        filters_map = self.map_field_names(filters)
        return_cols_map = self.map_field_names(return_cols)

        query = "SELECT"
        if distinct:
            query += " DISTINCT "
        query += "[" + "], [".join(return_cols_map) + "] "
        query += "FROM %s " % self.table
        if not filters_map is None and filters_map != {}:
            query += "WHERE "
            for i, kv in enumerate(filters_map.items()):
                k = kv[0]
                vs = kv[1]
                if isinstance(vs, str):
                    vs = self._sanitise(vs)
                    query += "[%s] = '%s' %s " % (k, vs, boolean)
                elif isinstance(vs, list):
                    for v in vs:
                        v = self._sanitise(v)
                        query += "[%s] = '%s' %s " % (k, v, boolean)
                elif isinstance(vs, int) or isinstance(vs, float):
                    query += "[%s] = %s %s " % (k, vs, boolean)
                else:
                    raise ValueError("Unsupported value type in key %s" % k)

            #remove final boolean keyword
            query = query.strip()[:(-1*len(boolean))]
        query = query.replace("[*]", "*")
        results = self.execute(query, select = True)

        if rc == "columns":
            # pivot each list in results to correspond to one column rather
            # than one row
            results_pivot = list(map(list, zip(*results)))

            if results_pivot == []:
                return {return_cols[i]: []
                        for i in range(len(return_cols))}
            else:
                return {return_cols[i]: results_pivot[i]
                        for i in range(len(return_cols))}
        elif rc == "rows":
            return [list(row) for row in results]
        else:
            raise ValueError("Invalid shape specified. rc must be one"
                             " of 'rows' or 'columns'.")

class MultiConnection:
    def __init__(self, db, tables = None, debug = False):
        self.db = db
        if tables is None:
            raise AttributeError("At least one table must be specified")

        elif not isinstance(tables, list):
            tables = [tables]

        for table in tables:
            self.__dict__[table] = TableCon(
                db = db, table = table, debug = debug)


if __name__ == "__main__":
    renames = TableCon(".\sqlite_db\insight", "renames", debug = True)
    renames.define_field_map({"Original name": "original_name", "#": "number"})

    print(renames.filter(filters = {'Composer': 'Beethoven, Ludwig Van'},
                         return_cols = ['composer', 'album'],
                         boolean = "AND",
                         rc = "columns"))
    renames.close()