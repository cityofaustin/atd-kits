"""
Utilities for interacting with the KITS advanced traffic management system's
MSSQL database.
"""
import pymssql


def get_conn(creds, max_tries=3):
    if max_tries > 5:
        raise Exception("Retry limit is 5")

    attempts = 0

    while attempts <= max_tries:
        attempts += 1

        try:
            conn = pymssql.connect(
                server=creds["server"],
                user=creds["user"],
                password=creds["password"],
                database=creds["database"],
                timeout=10,
            )

        except pymssql.OperationalError as e:
            if "Adaptive Server connection failed" in str(e) and attempts < max_tries:
                continue
            else:
                raise e

        return conn

def data_as_dict(creds, query, max_tries=3):
    conn = get_conn(creds, max_tries=max_tries)
    cursor = conn.cursor(as_dict=True)
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return data
