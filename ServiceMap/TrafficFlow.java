/* ServiceMap.
   Copyright (C) 2023 DISIT Lab http://www.disit.org - University of Florence

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as
   published by the Free Software Foundation, either version 3 of the
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>. */
package org.disit.servicemap.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.disit.servicemap.Configuration;
import org.disit.servicemap.ServiceMap;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 *
 * @author pierf
 */
public class TrafficFlow {

    public String trafficFlowSearch(String polygon, String dateObserved, String scenarioName,
            String roadElement, String kind) {
        String responseBody = "";

        try {
            Configuration conf = Configuration.getInstance();
            RestHighLevelClient client = ServiceMap.createElasticSearchClient(conf);
            String indexName = conf.get("elasticSearchRoadElementIndex", "roadelement");
            String sizeString = conf.get("elasticSearchRoadElementIndex", "2000");

            int size = Integer.parseInt(sizeString); //TODO FIX ME

            String query = constructQuery(size, polygon, dateObserved, scenarioName, roadElement, kind);
            Response response = executeElasticsearchQuery(client, query, indexName);
            System.out.println(query);
            if (response.getStatusLine().getStatusCode() == 200) {
                // Visualizza la risposta
                responseBody = convertStreamToString(response.getEntity().getContent());

                // Visualizza la stringa della risposta
                //System.out.println(responseBody);
                return responseBody;
            } else {
                System.err.println("Errore nella risposta Elasticsearch: " + response.getStatusLine().getReasonPhrase());
            }
        } catch (IOException e) {
            System.out.println("Exception TrafficFlowSearch: " + e);

            return "Smething gone wrong";
        }
        return responseBody;

    }

    private String constructQuery(int size, String polygon, String dateObserved, String scenarioName,
            String roadElement, String kind) {
        // Costruisco progressivamente il json della query Elastic

        String query = "{"
                + "\"size\": " + size + ","
                + "\"query\": {"
                + "\"bool\": {"
                + "\"must\": [";

        if (dateObserved != null) {

            if (scenarioName == null && roadElement == null && kind == null) { // se è l'unico chiudo la clausola "must"
                query += "{"
                        + "\"match\": {"
                        + "\"dateObserved\": \"" + dateObserved + "\""
                        + "}"
                        + "}]";
            } else {
                query += "{"
                        + "\"match\": {"
                        + "\"dateObserved\": \"" + dateObserved + "\""
                        + "}"
                        + "},";
            }
        }

        if (scenarioName != null) {
            if (roadElement == null && kind == null) {
                query += "{"
                        + "\"match\": {"
                        + "\"scenario\": \"" + scenarioName + "\""
                        + "}"
                        + "}]";
            } else {
                query += "{"
                        + "\"match\": {"
                        + "\"scenario\": \"" + scenarioName + "\""
                        + "}"
                        + "},";
            }
        }
        
        if(kind != null){
            if (roadElement == null) {
                query += "{"
                        + "\"match\": {"
                        + "\"kind\": \"" + kind + "\""
                        + "}"
                        + "}]";
            } else {
                query += "{"
                        + "\"match\": {"
                        + "\"kind\": \"" + kind + "\""
                        + "}"
                        + "},";
            }
        }
        
        if (roadElement != null) {
            query += "{"
                    + "\"match\": {"
                    + "\"roadElements\": \"" + roadElement + "\""
                    + "}}"
                    + "]";
        }

        if (polygon != null) {
            if (dateObserved == null && scenarioName == null && roadElement == null && kind == null) { // se è l'unicio non uso la
                // clausola "must"
                query = "{"
                        + "\"size\": " + size + ","
                        + "\"query\": {"
                        + "\"bool\": {"
                        + "\"filter\": ["
                        + "{"
                        + "\"geo_shape\": {"
                        + "\"line\": {"
                        + "\"shape\": \"" + polygon + "\","
                        + "\"relation\": \"intersects\""
                        + "}}}"
                        + "]";
            } else {
                query += ",\"filter\": ["
                        + "{"
                        + "\"geo_shape\": {"
                        + "\"line\": {"
                        + "\"shape\": \"" + polygon + "\","
                        + "\"relation\": \"intersects\""
                        + "}}}"
                        + "]";
            }
        }

        query += "}}}";

        return query;
    }

    private Response executeElasticsearchQuery(RestHighLevelClient client, String query,
            String indexName)
            throws IOException {
        Response response = null;

        String endpoint = "/" + indexName + "/_search";
        // Esegui la richiesta personalizzata POST
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(query);

        // Esegui la richiesta Elasticsearch
        response = client.getLowLevelClient().performRequest(request);

        // Chiudi il client
        client.close();

        return response;
    }

    private String convertStreamToString(InputStream is) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                //System.out.println("linea: " + line);
                sb.append(line).append('\n');
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    public boolean wktValidator(String wktToValidate) {

        try {
            WKTReader reader = new WKTReader();

            Geometry geometry = reader.read(wktToValidate);

            return true;
        } catch (Exception e) {
            System.err.println("WKT not valid: " + e.getMessage());
            return false;
        }
    }
}
