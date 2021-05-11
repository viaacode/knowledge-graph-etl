import os.path
from datetime import timedelta
from re import template

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable
from airflow.models.connection import Connection
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from construction_site import parse_json
from psycopg2 import sql
from rdflib import Graph, Namespace
from SPARQLWrapper import DIGEST, POST, SPARQLWrapper
from SPARQLWrapper.Wrapper import BASIC
from urllib.parse import quote_plus

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
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

DIR = Variable.get("data_path", "./files")
SRC_NS = Variable.get("source_ns", "https://data.meemoo.be/sources/")

with DAG(
    "knowledge-graph-etl",
    default_args=default_args,
    description="ETL to extract, map and load JSON data into an RDF triple store.",
    schedule_interval=None,  # timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
    user_defined_macros={
        'quote_plus': quote_plus,
        'list_to_nt': lambda l: ', '.join(list(map(lambda u: '<{}>'.format(u), l)))
    }
) as dag:

    def _get_cursor(postgres_conn_id, schema, table, field, records=True):

        if records:
            template = "SELECT {}::text FROM {}"
        else:
            template = "SELECT jsonb_agg({})::text FROM {}"

        query = sql.SQL(template).format(
            sql.Identifier(table, field), sql.Identifier(schema, table)
        )

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.itersize = 10000  # chunk size

        # test data retrieval
        print("Retrieved cursor for {}".format(query))
        cursor.execute(query)
        return cursor

    def _to_nt(t, namespace_manager=None):
        return "{} {} {} . \n".format(
            t[0].n3(namespace_manager),
            t[1].n3(namespace_manager),
            t[2].n3(namespace_manager),
        )

    def extract_json(ds, **kwargs):
        """Extract the JSON data from the postgres database."""

        postgres_conn_id = kwargs.get("postgres_conn_id")
        schema = kwargs.get("schema")
        table = kwargs.get("table")
        field = kwargs.get("field")
        filename = kwargs.get("filename")

        cursor = _get_cursor(postgres_conn_id, schema, table, field, False)
        outfile = open(filename, "w")
        outfile.write(cursor.fetchone()[0])
        # Closing file
        outfile.close()

        print(
            "JSON records from {}.{} been written to {}.".format(
                schema, table, filename
            )
        )

    def extract_json_as_rdf(ds, **kwargs):
        """Extract the JSON data from the postgres database and map it on the fly."""

        postgres_conn_id = kwargs.get("postgres_conn_id")
        schema = kwargs.get("schema")
        table = kwargs.get("table")
        field = kwargs.get("field")
        filename = kwargs.get("filename")
        namespace = kwargs.get("namespace")

        cursor = _get_cursor(postgres_conn_id, schema, table, field)
        outfile = open(filename, "w")

        ns = Namespace(namespace)
        g = Graph()
        g.bind("source", ns)

        outfile.write("@prefix source: <{}> . \n".format(namespace))

        for record in cursor:
            nr_of_triples = 0
            for t in parse_json(record[0], namespace=ns):
                outfile.write(_to_nt(t, namespace_manager=g.namespace_manager))
                nr_of_triples += 1
            print(
                "Record {} produced {} triples.".format(cursor.rownumber, nr_of_triples)
            )
        # Closing file
        outfile.close()

        print(
            "{} JSON records from {}.{} been written to {}.".format(
                cursor.rowcount, schema, table, filename
            )
        )

    def extract_and_insert(ds, **kwargs):
        """Extract the JSON data from the postgres database and map it on the fly."""

        postgres_conn_id = kwargs.get("postgres_conn_id")
        schema = kwargs.get("schema")
        table = kwargs.get("table")
        field = kwargs.get("field")
        namespace = kwargs.get("namespace")
        http_conn_id = kwargs.get("http_conn_id")
        method = kwargs.get("method", POST)
        auth_type = kwargs.get("auth_type", DIGEST)

        cursor = _get_cursor(postgres_conn_id, schema, table, field)
        for record in cursor:
            sparql = "INSERT DATA {{"

            for t in parse_json(record[0], namespace=namespace):
                sparql += _to_nt(t)

            sparql = "}}"

            result = sparql_update(
                ds,
                query=sparql,
                http_conn_id=http_conn_id,
                auth_type=auth_type,
                method=method,
            )
            print("Executed: {} ({})".format(sparql, result))

        print(
            "JSON records from {}.{} have been inserted in {}.".format(
                schema, table, http_conn_id
            )
        )

    def sparql_update(ds, **kwargs):
        """Execute a sparql query on a sparql endpoint."""

        http_conn_id = kwargs.get("http_conn_id")

        if kwargs["templates_dict"] is not None and "query" in kwargs["templates_dict"]:
            query = kwargs["templates_dict"]["query"]
        else:
            query = kwargs.get("query")

        method = kwargs.get("method", POST)
        auth_type = kwargs.get("auth_type", DIGEST)

        query_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), query)
        if os.path.isfile(query_path):
            with open(query_path) as f:
                query = f.read()
        else:
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

        print("Query: {}".format(query))

        sparql = SPARQLWrapper(endpoint)

        sparql.setHTTPAuth(auth_type)
        sparql.setCredentials(conn.login, conn.password)
        sparql.setMethod(method)
        sparql.setQuery(query)

        results = sparql.query()
        print(results.response.read())


    # Turn all JSON data into RDF files
    e1 = PythonOperator(
        task_id="ldap_organizations_extract_json",
        python_callable=extract_json_as_rdf,
        op_kwargs={
            "schema": "shared",
            "table": "ldap_organizations",
            "field": "ldap_content",
            "filename": DIR + "/ldap_organizations.ttl",
            "postgres_conn_id": "avo2",
            "namespace": SRC_NS,
        },
    )

    e2 = PythonOperator(
        task_id="ldap_entities_extract_json",
        python_callable=extract_json_as_rdf,
        op_kwargs={
            "schema": "shared",
            "table": "ldap_entities",
            "field": "ldap_content",
            "filename": DIR + "/ldap_entities.ttl",
            "postgres_conn_id": "avo2",
            "namespace": SRC_NS,
        },
    )

    e3 = PythonOperator(
        task_id="tl_users_extract_json",
        python_callable=extract_json_as_rdf,
        op_kwargs={
            "schema": "shared",
            "table": "tl_users",
            "field": "tl_content",
            "filename": DIR + "/tl_users.ttl",
            "postgres_conn_id": "avo2",
            "namespace": SRC_NS,
        },
    )

    e4 = PythonOperator(
        task_id="tl_companies_extract_json",
        python_callable=extract_json_as_rdf,
        op_kwargs={
            "schema": "shared",
            "table": "tl_companies",
            "field": "tl_content",
            "filename": DIR + "/tl_companies.ttl",
            "postgres_conn_id": "avo2",
            "namespace": SRC_NS,
        },
    )

    # load

    l1 = PythonOperator(
        task_id="ldap_organizations_load",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": "sparql_endpoint"},
        templates_dict={
            "query": "LOAD <file://{{params.file}}> INTO GRAPH <{{params.graph}}>"
        },
        params={
            "file": "/data/ldap_organizations.ttl",
            "graph": "https://data.meemoo.be/graphs/ldap_organizations",
        },
    )

    l2 = PythonOperator(
        task_id="ldap_entities_load",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": "sparql_endpoint"},
        templates_dict={
            "query": "LOAD <file://{{params.file}}> INTO GRAPH <{{params.graph}}>"
        },
        params={
            "file": "/data/ldap_entities.ttl",
            "graph": "https://data.meemoo.be/graphs/ldap_entities",
        },
    )

    l3 = PythonOperator(
        task_id="tl_users_load",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": "sparql_endpoint"},
        templates_dict={
            "query": "LOAD <file://{{params.file}}> INTO GRAPH <{{params.graph}}>"
        },
        params={
            "file": "/data/tl_users.ttl",
            "graph": "https://data.meemoo.be/graphs/tl_users",
        },
    )

    l4 = PythonOperator(
        task_id="tl_companies_load",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": "sparql_endpoint"},
        templates_dict={
            "query": "LOAD <file://{{params.file}}> INTO GRAPH <{{params.graph}}>"
        },
        params={
            "file": "/data/tl_companies.ttl",
            "graph": "https://data.meemoo.be/graphs/tl_companies",
        },
    )

    c = PythonOperator(
        task_id="clear_org_graph",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": "sparql_endpoint"},
        templates_dict={"query": "CLEAR GRAPH <{{params.graph}}>"},
        params={"graph": "https://data.meemoo.be/graphs/organizations"},
    )

    m1 = PythonOperator(
        task_id="ldap_mapping_orgs",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_orgs.sparql",
            "http_conn_id": "sparql_endpoint",
        },
    )

    m2 = PythonOperator(
        task_id="tl_users_mapping_1",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_users_mapping_1.sparql",
            "http_conn_id": "sparql_endpoint",
        },
    )

    m3 = PythonOperator(
        task_id="tl_companies_mapping_orgs",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_companies_mapping_orgs.sparql",
            "http_conn_id": "sparql_endpoint",
        },
    )

    m4 = PythonOperator(
        task_id="ldap_mapping_schools",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_schools.sparql",
            "http_conn_id": "sparql_endpoint",
        },
    )

    m5 = PythonOperator(
        task_id="ldap_mapping_eduorg",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_eduorg.sparql",
            "http_conn_id": "sparql_endpoint",
        },
    )

    m6 = PythonOperator(
        task_id="add_provenance",
        python_callable=sparql_update,
        templates_dict={
            "query": """
            PREFIX prov: <http://www.w3.org/ns/prov#>
            PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#> 
            PREFIX : <http://example.org/>

            INSERT DATA 
            {
                GRAPH <{{params.graph}}> {
                    <{{params.result}}> prov:wasDerivedFrom {{ list_to_nt(params.sources)}};
                                        prov:wasGeneratedBy :{{ quote_plus(run_id) }}.
                                         
                    :{{ quote_plus(run_id) }} a prov:Activity, :AirflowRun;
                        prov:generated <{{params.result}}>;
                        prov:used          :ApacheAirflow;
                        prov:startedAtTime "{{ ts }}"^^xsd:dateTime.
                    
                }
            }
            """,
        },
        params={
            "sources": [
                "https://data.meemoo.be/graphs/tl_companies", 
                "https://data.meemoo.be/graphs/tl_users", 
                "https://data.meemoo.be/graphs/ldap_organizations", 
                "https://data.meemoo.be/graphs/ldap_entities"
            ],
            "result": "https://data.meemoo.be/graphs/organizations",
            "graph": "https://data.meemoo.be/graphs/provenance",
        },
        op_kwargs={"http_conn_id": "sparql_endpoint"},
    )

    e1 >> l1
    e2 >> l2
    e3 >> l3
    e4 >> l4

    l1 >> [m1, m4, m5]
    l2 >> [m1, m4, m5]
    l3 >> m2
    l4 >> m3
    [l1, l2, l3, l4] >> c >> m6
    c >> [m1, m2, m3, m4, m5]
