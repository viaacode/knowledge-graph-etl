
from datetime import timedelta
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST
from SPARQLWrapper.Wrapper import BASIC

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from psycopg2 import sql
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import HttpHook
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    def extract_json(postgres_conn_id, schema, table, field, filename, itersize = 5000):
        """Extract the JSON data from the postgres database."""

        query = sql.SQL("SELECT {}::text FROM {}").format(
            sql.Identifier(table, field),
            sql.Identifier(schema, table))

        outfile = open(filename,'w')

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        with pg_hook.get_conn() as conn:
            print(query.as_string(conn))
            with conn.cursor() as cursor:

                cursor.itersize = itersize # chunk size

                #test data retrieval
                cursor.execute(query)
                outfile.write('[')
                for record in cursor:
                    if cursor.rownumber > 1:
                        outfile.write(',')
                    outfile.write(str(record[0]))
                outfile.write(']')
        
        # Closing file
        outfile.close()
        return "Records from {}.{} been written to {}.".format(schema, table, filename)

    def sparql_update(ds, **kwargs):
        """Execute a sparql query on a sparql endpoint."""

        http_conn_id = kwargs.get('http_conn_id')
        query = kwargs.get('query')
        method = kwargs.get('method', POST)
        auth_type = kwargs.get('auth_type', DIGEST)

        try:
            with open(query) as f:
                query = f.read()
        except FileNotFoundError:
            print("Query does not point to a file; executing as query text.")
        
        conn = HttpHook.get_connection(http_conn_id)

        if conn.host and "://" in conn.host:
            endpoint = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            if conn.host:
                host = conn.host  
            else:
                raise ValueError("Host cannot be empty")
            endpoint = schema + "://" + host

        sparql = SPARQLWrapper(endpoint)

        sparql.setHTTPAuth(auth_type)
        sparql.setCredentials(conn.login, conn.password)
        sparql.setMethod(method)
        sparql.setQuery(query)

        results = sparql.query()
        return results.response.read()

    extract_json_task = PythonOperator(
        task_id='extract_json',
        python_callable=extract_json,
        op_kwargs= {
            'schema': 'shared', 
            'table': 'ldap_organizations', 
            'field': 'ldap_content', 
            'filename': 'ldap.json',
            'postgres_conn_id': 'avo2'
        }
    )

    json_to_rdf_task = BashOperator(
        task_id='json_to_rdf',
        bash_command='cat {{params.json_file}} | docker run -i -a stdin -a stdout -a stderr atomgraph/json2rdf {{params.namespace}} | riot --formatted=TURTLE > {{params.rdf_file}}',
        params={
            'json_file' : 'ldap.json',
            'rdf_file' : 'ldap.ttl',
            'namespace' : 'https://data.meemoo.be/source/'
        },
    )
    
    load_rdf_task = PythonOperator(
        task_id='load_rdf',
        python_callable=sparql_update,
        op_kwargs={
            'http_conn_id': 'sparql_endpoint'
        },
        templates_dict={
            'query': 'LOAD <file://{{params.file}}> INTO GRAPH <{{params.graph}}>' 
        },
        params={
            'file': '/data/ldap.ttl',
            'graph': 'http://data.meemoo.be/graphs/ldap-source'
        }
    )

    map_rdf_task = PythonOperator(
        task_id='map_rdf',
        python_callable=sparql_update,
        op_kwargs={
            'query': 'sparql/ldap-mapping.sparql', 
            'http_conn_id': 'sparql_endpoint'
        }
    )



    