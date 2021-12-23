import json
import os.path
from datetime import timedelta
from urllib.parse import quote_plus

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from psycopg2 import sql
from rdflib import Graph, Namespace

from parse_functions import parse_json
from sparql_update import SparqlUpdateHook

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
}

DIR = Variable.get("data_path", "./files")
SRC_NS = Variable.get("source_ns", "https://data.meemoo.be/sources/")

teamleader2db_conn_id = Variable.get("teamleader2db_conn_id", "teamleader2db-qas")
ldap2db_conn_id = Variable.get("ldap2db_conn_id", "ldap2db-qas")
endpoint_conn_id = Variable.get("endpoint_conn_id", "stardog-qas")
postgres_conn_id = Variable.get("postgres_conn_id", "etl-harvest-qas")
full_sync = Variable.get("full_sync", False, True)

with DAG(
    "knowledge-graph-etl",
    default_args=default_args,
    description="ETL to extract, map and load JSON data into an RDF triple store.",
    schedule_interval=None,  # timedelta(days=1),
    start_date=days_ago(2),
    tags=["example"],
    user_defined_macros={
        "quote_plus": quote_plus,
        "list_to_nt": lambda l: ", ".join(list(map(lambda u: "<{}>".format(u), l))),
    },
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
                outfile.write(
                    SparqlUpdateHook.to_ntriples(
                        t, namespace_manager=g.namespace_manager
                    )
                )
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
        """Extract the JSON data from the postgres database, map it, and directly insert it."""

        postgres_conn_id = kwargs.get("postgres_conn_id")
        schema = kwargs.get("schema")
        table = kwargs.get("table")
        field = kwargs.get("field")
        namespace = kwargs.get("namespace")
        http_conn_id = kwargs.get("http_conn_id")
        graph = kwargs.get("graph", None)

        cursor = _get_cursor(postgres_conn_id, schema, table, field)
        hook = SparqlUpdateHook(method="POST", http_conn_id=http_conn_id)

        for record in cursor:
            triples_gen = parse_json(record[0], namespace=Namespace(namespace))
            hook.insert(triples_gen, graph)

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

        query_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), query)
        if os.path.isfile(query_path):
            with open(query_path) as f:
                query = f.read()
        else:
            print("Query does not point to a file; executing as query text.")

        SparqlUpdateHook(method="POST", http_conn_id=http_conn_id).sparql_update(query)

    # Turn all JSON data into RDF and insert
    # TODO: using graph store protocol is probably better than SPARQL update INSERT statements

    h0 = HttpSensor(
        task_id="teamleader2db_check",
        http_conn_id=teamleader2db_conn_id,
        endpoint="",
        request_params={},
        response_check=lambda response: not response.json()["job_running"],
        poke_interval=5,
    )

    h1 = SimpleHttpOperator(
        task_id="teamleader2db_run",
        http_conn_id=teamleader2db_conn_id,
        method="POST",
        endpoint="",
        data=json.dumps({"full_sync": full_sync}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.json()["status"]
        == "Teamleader sync started",
    )

    h2 = HttpSensor(
        task_id="teamleader2db_run_check",
        http_conn_id=teamleader2db_conn_id,
        endpoint="",
        request_params={},
        response_check=lambda response: not response.json()["job_running"],
        poke_interval=5,
    )

    h3 = HttpSensor(
        task_id="ldap2db_check",
        http_conn_id=ldap2db_conn_id,
        endpoint="",
        request_params={},
        response_check=lambda response: not response.json()["job_running"],
        poke_interval=5,
    )

    h4 = SimpleHttpOperator(
        task_id="ldap2db_run",
        http_conn_id=ldap2db_conn_id,
        method="POST",
        endpoint="",
        data=json.dumps({"full_sync": full_sync}),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.json()["status"]
        == "DEEWEE sync started",
    )

    h5 = HttpSensor(
        task_id="ldap2db_run_check",
        http_conn_id=ldap2db_conn_id,
        endpoint="",
        request_params={},
        response_check=lambda response: not response.json()["job_running"],
        poke_interval=5,
    )

    e1 = PythonOperator(
        task_id="ldap_organizations_extract_json",
        python_callable=extract_and_insert,
        op_kwargs={
            "schema": "public",
            "table": "ldap_organizations",
            "field": "ldap_content",
            "postgres_conn_id": postgres_conn_id,
            "http_conn_id": endpoint_conn_id,
            "namespace": SRC_NS,
            "graph": "https://data.meemoo.be/graphs/ldap_organizations",
        },
    )

    e2 = PythonOperator(
        task_id="tl_users_extract_json",
        python_callable=extract_and_insert,
        op_kwargs={
            "schema": "public",
            "table": "tl_users",
            "field": "tl_content",
            "postgres_conn_id": postgres_conn_id,
            "http_conn_id": endpoint_conn_id,
            "namespace": SRC_NS,
            "graph": "https://data.meemoo.be/graphs/tl_users",
        },
    )

    e3 = PythonOperator(
        task_id="tl_companies_extract_json",
        python_callable=extract_and_insert,
        op_kwargs={
            "schema": "public",
            "table": "tl_companies",
            "field": "tl_content",
            "postgres_conn_id": postgres_conn_id,
            "http_conn_id": endpoint_conn_id,
            "namespace": SRC_NS,
            "graph": "https://data.meemoo.be/graphs/tl_companies",
        },
    )

    # clear graphs
    c1 = PythonOperator(
        task_id="ldap_organizations_clear",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": endpoint_conn_id},
        templates_dict={"query": "CLEAR SILENT GRAPH <{{params.graph}}>"},
        params={
            "graph": "https://data.meemoo.be/graphs/ldap_organizations",
        },
    )

    c2 = PythonOperator(
        task_id="tl_users_clear",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": endpoint_conn_id},
        templates_dict={"query": "CLEAR SILENT GRAPH <{{params.graph}}>"},
        params={
            "graph": "https://data.meemoo.be/graphs/tl_users",
        },
    )

    c3 = PythonOperator(
        task_id="tl_companies_clear",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": endpoint_conn_id},
        templates_dict={"query": "CLEAR SILENT GRAPH <{{params.graph}}>"},
        params={
            "graph": "https://data.meemoo.be/graphs/tl_companies",
        },
    )

    c = PythonOperator(
        task_id="clear_org_graph",
        python_callable=sparql_update,
        op_kwargs={"http_conn_id": endpoint_conn_id},
        templates_dict={"query": "CLEAR SILENT GRAPH <{{params.graph}}>"},
        params={"graph": "https://data.meemoo.be/graphs/organizations"},
    )

    # map by running sparql
    m1 = PythonOperator(
        task_id="map_ldap_orgs",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_orgs.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m2 = PythonOperator(
        task_id="map_tl_users",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_users_mapping.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m3 = PythonOperator(
        task_id="map_tl_companies_orgs",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_companies_mapping_orgs.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m4 = PythonOperator(
        task_id="map_ldap_schools",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_schools.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m5 = PythonOperator(
        task_id="map_ldap_eduorg",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_eduorg.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m6 = PythonOperator(
        task_id="map_tl_companies_contactpoints",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_companies_mapping_contactpoints.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m7 = PythonOperator(
        task_id="map_tl_companies_cps",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_companies_mapping_cps.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m8 = PythonOperator(
        task_id="map_tl_companies_classification",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/tl_companies_mapping_classification.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    m9 = PythonOperator(
        task_id="map_ldap_cps",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/ldap_mapping_cps.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    mt = PythonOperator(
        task_id="insert_mam_tenants",
        python_callable=sparql_update,
        op_kwargs={
            "query": "sparql/mam_tenants_prd.sparql",
            "http_conn_id": endpoint_conn_id,
        },
    )

    mp = PythonOperator(
        task_id="add_provenance",
        python_callable=sparql_update,
        templates_dict={
            "query": """
            PREFIX prov: <http://www.w3.org/ns/prov#>
            PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#> 
            PREFIX : <https://data.meemoo.be/>

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
            ],
            "result": "https://data.meemoo.be/graphs/organizations",
            "graph": "https://data.meemoo.be/graphs/provenance",
        },
        op_kwargs={"http_conn_id": endpoint_conn_id},
    )

    h0 >> h1 >> h2 >> [c1, c2, c3]
    h3 >> h4 >> h5 >> [c1, c2, c3]

    c1 >> e1
    c2 >> e2
    c3 >> e3

    e1 >> [m1, m4, m5, m9]
    e2 >> m2
    e3 >> [m3, m6, m7, m8]

    [e1, e2, e3] >> c >> mp
    c >> [m1, m2, m3, m4, m5, m6, m7, m8, m9, mt]
