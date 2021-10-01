#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os.path
from typing import Any, Dict, Optional

from airflow.providers.http.operators.http import HttpHook
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from SPARQLWrapper import DIGEST, SPARQLWrapper, POST, GET
from SPARQLWrapper.Wrapper import BASIC

AUTH_TYPES = {HTTPBasicAuth: BASIC, HTTPDigestAuth: DIGEST}
METHODS = {"GET": GET, "POST": POST}


class SparqlUpdateHook(HttpHook):
    """
    Interact with SPARQL endpoints.
    :param method: the API method to be called
    :type method: str
    :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the
        SPARQL endpoint url i.e https://dbpedia.org/sparql and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :type http_conn_id: str
    :param auth_type: The auth type for the service
    :type auth_type: AuthBase of python requests lib
    """

    default_conn_name = "sparql_endpoint"
    hook_name = "SPARQLUpdate"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = HTTPBasicAuth,
    ) -> None:
        super().__init__(method, http_conn_id, auth_type)

        conn = self.get_connection(self.http_conn_id)

        if conn.host and "://" in conn.host:
            self.endpoint = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            if conn.host:
                host = conn.host
            else:
                self.log.error()
                raise ValueError("Host cannot be empty")
            self.endpoint = schema + "://" + host

        self.sparql = SPARQLWrapper(self.endpoint)

        if self.auth_type in AUTH_TYPES:
            self.sparql.setHTTPAuth(AUTH_TYPES[self.auth_type])

        self.sparql.setCredentials(conn.login, conn.password)
        self.sparql.setMethod(METHODS[self.method])

    def sparql_update(self, query: str, headers: Optional[Dict[str, Any]] = None):
        """Execute a sparql query on a sparql endpoint.
        :param query: SPARQL Update query to be uploaded or request parameters
        :type query: str
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """

        query_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), query)
        if os.path.isfile(query_path):
            with open(query_path) as f:
                query = f.read()
        else:
            self.log.warning("Query does not point to a file; executing as query text.")

        self.sparql.setQuery(query)

        if not self.sparql.isSparqlUpdateRequest():
            self.log.warning("Query is not an update query.")

        if headers is not None:
            for h in headers.items():
                self.sparql.addCustomHttpHeader(h[0], h[1])

        self.log.info("Sending query '{}' to {}".format(query, self.endpoint))

        results = self.sparql.query()
        self.log.info(results.response.read())

        self.sparql.resetQuery()

    def insert(self, triples, graph=None):
        query = "INSERT DATA {\n"

        if graph is not None:
            query += "GRAPH <{}> {{\n".format(graph)

        for t in triples:
            query += self.to_ntriples(t)

        query += "}\n"

        if graph is not None:
            query += "}"

        self.sparql_update(query)

    @staticmethod
    def to_ntriples(t, namespace_manager=None):
        return "{} {} {} . \n".format(
            t[0].n3(namespace_manager),
            t[1].n3(namespace_manager),
            t[2].n3(namespace_manager),
        )
