class CommonDBOperationsMixin:
    """
    Wrapper class for some common DB operations.

    """

    def _format_query(self, query, parameters):
        if parameters:
            query = query.format(**parameters)
        return query

    def get_records(self, hook, query, parameters=None):
        """
        Execute a SELECT query and return ROWS only.

        :param hook: Hook initialized with "conn_id". For example: PostgresHook(rs_conn_id) or MySqlHook(ms_conn_id)
        :param query: SELECT query to be executed, optionally parameterized.
        :param parameters: Optional query parameters to format `query`.
        :return: ROWS
        """
        query = self._format_query(query, parameters)
        rows = hook.get_records(sql=query)

        return rows

    def get_records_as_df(self, hook, query, parameters=None):
        """
        Execute a SELECT query and return pandas DATAFRAME

        :param hook: Hook initialized with "conn_id". For example: PostgresHook(rs_conn_id) or MySqlHook(ms_conn_id)
        :param query: SELECT query to be executed, optionally parameterized.
        :param parameters: Optional query parameters to format `query`.
        :return: pandas dataframe
        """
        query = self._format_query(query, parameters)
        df = hook.get_pandas_df(sql=query)

        return df

    def get_records_and_headers(self, hook, query, parameters=None):
        """
        Execute a SELECT query and return ROWS and HEADERS (column names).

        :param hook: Hook initialized with "conn_id". For example: PostgresHook(rs_conn_id) or MySqlHook(ms_conn_id)
        :param query: SELECT query to be executed, optionally parameterized.
        :param parameters: Optional query parameters to format `query`.
        :return: ROWS and HEADERS as lists.
        """

        query = self._format_query(query, parameters)
        cursor = hook.get_cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [i[0] for i in cursor.description]

        return rows, headers

    def get_one(self, hook, query, parameters=None):
        """
        Fetches single record from results set of select statement.

        :param hook: Hook initialized with "conn_id". For example: PostgresHook(rs_conn_id) or MySqlHook(ms_conn_id)
        :param query: SELECT query to be executed, optionally parameterized.
        :param parameters: Optional query parameters to format `query`.
        :return: record
        """

        query = self._format_query(query, parameters)
        row = hook.get_first(query)

        return row

    def execute_query(self, hook, query, parameters=None):
        """
        Execute SQL statement(s) -> INSERTS/UPDATES/DELETES etc.

        Returns no records.

        :param hook: Hook initialized with "conn_id". For example: PostgresHook(rs_conn_id) or MySqlHook(ms_conn_id)
        :param query: SELECT query to be executed, optionally parameterized.
        :param parameters: Optional query parameters to format `query`.
        :return: None
        """

        query = self._format_query(query, parameters)
        cursor = hook.get_cursor()
        cursor.execute(query)

        return None

    def execute_query_with_commit(self, hook, query, parameters=None, autocommit=True):
        """
        Execute a query using autocommit mode.

        :param hook: Hook initialized with 'conn_id'. For example: PostgresHook(rs_conn_id) or MySqlHook(ms_conn_id)
        :param query: SELECT query to be executed, optionally parameterized.
        :param parameters: Optional query parameters to format `query`.
        :param autocommit: Autocommit true/false

        :return: None
        """

        query = self._format_query(query, parameters)
        hook.run(sql=query, parameters=parameters, autocommit=autocommit)

        return None
