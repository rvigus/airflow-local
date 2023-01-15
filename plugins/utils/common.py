from sqlalchemy import create_engine
from airflow.models import Variable

def get_postgres_engine(conn):
    return create_engine(Variable.get(conn))

def read_sql(path: str, multi=False):
    """
    Read a file at a given path
    :param path: filepath string
    :return: query string
    """
    try:
        with open(path, "r") as f:
            contents = f.read()

        if multi:
            statements = contents.split(';')
            return [i for i in statements if i != '']
        else:
            return contents
    except FileNotFoundError:
        raise
    except Exception:
        raise