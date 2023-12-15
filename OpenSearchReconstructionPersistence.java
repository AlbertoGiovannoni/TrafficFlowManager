package org.disit.TrafficFlowManager.persistence;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonObject;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.util.EntityUtils;
import org.disit.TrafficFlowManager.utils.ConfigProperties;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class OpenSearchReconstructionPersistence {

    private static volatile CloseableHttpClient httpClient = HttpClients.createDefault();
    private static volatile RestClientBuilder builder;

    // class to count the number of error occurred in this session
    private static class ErrorCounter {
        private int errorCount = 0;

        public synchronized void incrementErrorCount() {
            errorCount++;
        }

        public int getErrorCount() {
            return errorCount;
        }
    }

    public static void sendEs(JSONObject dinamico, String kind) throws Exception {

        // Path currentRelativePath = Paths.get("");
        // String path = currentRelativePath.toAbsolutePath().toString();
        // String filePath = path + "/src/data/conf.json";
        try {
            // JSONObject conf = new JSONObject(new
            // String(Files.readAllBytes(Paths.get(filePath))));
            Properties conf = ConfigProperties.getProperties();
            String url = conf.getProperty("opensearchHostname");
            if (url == null) {
                System.out.println("opensearchHostname not specified in configuration NOT SENDING to opensearch");
                return;
            }
            String[] hostnames = url.split(";");
            int port = 9200;
            String admin = conf.getProperty("opensearchUsername", "admin");
            String password = conf.getProperty("opensearchPassw", "password");

            String kbUrl = conf.getProperty("kbUrl", "https://www.disit.org/smosm/sparql?format=json");

            int batchSize = Integer.parseInt(conf.getProperty("opensearchBatchSize", "150"));
            int threadNumber = Integer.parseInt(conf.getProperty("opensearchThreadNumber", "150"));
            int maxErrors = Integer.parseInt(conf.getProperty("opensearchMaxErrors", "20"));

            String indexName = conf.getProperty("opensearchIndexName", "roadelement2");

            // split road elements

            // Invio all'indice per segmento

            System.out.println("processing dinamic json");
            // JSONArray jd20Splitted = new JSONArray(jd20);

            JSONArray jd20 = preProcess(dinamico, kind);

            JSONArray reArray = new JSONArray();
            System.out.println("pre-processing JD20");

            reArray = splitRoadElement(jd20);

            Map<String, Double> densityAverageMap = new HashMap<>();
            densityAverageMap = mapDensity(reArray);

            System.out.println("building inverted JD20");
            JSONArray invertedArray = new JSONArray();

            invertedArray = invertedIndex(reArray, densityAverageMap);

            System.out.println("retrieving road element coordinates in KB");

            // int batchSize = conf.getInt("batch_size");// descrive il numero di road
            // element ricercati in ogni query

            // int threadNumber = conf.getInt("thread_number");
            long start = System.currentTimeMillis();

            // creo un unico httpClient per tutte le richieste
            // CloseableHttpClient httpClient = HttpClients.createDefault();

            JSONObject coord = getCoord(invertedArray, batchSize, kbUrl, httpClient);
            System.out.println("time retrieving road element coordinates in KB: " + (System.currentTimeMillis() - start)
                    + " ms");

            JSONArray coordArray = coord.getJSONArray("results");

            PostProcessRes result = postProcess(invertedArray, coordArray);
            invertedArray = result.getPostPorcessData();
            // invertedArray = postProcess(invertedArray, coordArray);

            // creo indice
            // createIndex(indexName, url, port, admin, password);

            // gestisco inserimenti con thread

            // create object ErrorCounter
            // ErrorCounter errorCounter = new ErrorCounter();

            // Inizializza l'oggetto ErrorCounter condiviso
            ErrorCounter errorCounter = new ErrorCounter();

            System.out.println("indexing documents in elasticSearch");

            System.out.println("errorCounter at the start: " + errorCounter.errorCount);

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(admin, password));

            builder = RestClient.builder(
                    new HttpHost(url, port, "https"))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            // Configura l'autenticazione HTTP
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            return httpClientBuilder;
                        }
                    });

            sendToIndex(threadNumber, invertedArray, indexName, hostnames, port, admin, password, maxErrors,
                    errorCounter, builder);
            System.out.println("errorCounter at the end: " + errorCounter.errorCount);
            System.out.println("done");

        } catch (JSONException e) {
            System.out.println("Error in the main: " + e);
            throw new Exception("Failed to send data to ES: " + e);
        }
    }
    // ############################################# PROCESSING METHODS

    private static JSONArray preProcess(JSONObject dinamic, String kind) throws Exception {
        try {
            String tmp = "{\"scenario\":\"\",\"dateObserved\":\"\",\"segment\":\"\",\"dir\":0,\"roadElements\":[],\"start\":{\"location\":{\"lon\":\"\",\"lat\":\"\"}},\"end\":{\"location\":{\"lon\":\"\",\"lat\":\"\"}},\"flow\":\"\",\"density\":0,\"numVehicle\":\"\"}";
            JSONObject template = new JSONObject(tmp);
            JSONObject reconstructionData = dinamic.getJSONObject("reconstructionData");
            JSONObject metadata = dinamic.getJSONObject("metadata");

            template.put("scenario", metadata.getString("scenarioID"));
            template.put("dateObserved", metadata.getString("dateTime"));
            template.put("kind", kind);
            JSONArray JD20 = new JSONArray();

            for (Object key : reconstructionData.keySet()) { // itero sulle road
                String field = (String) key;
                JSONObject fieldData = (JSONObject) reconstructionData.get(field);
                JSONArray data = (JSONArray) fieldData.get("data");

                for (Object item : data) { // itero sui segment

                    JSONObject itemData = (JSONObject) item;
                    for (Object itemKey : itemData.keySet()) {
                        int dir = 0;

                        JSONObject tmpTemplate = new JSONObject(template.toString()); // deep copy del template
                        String itemName = (String) itemKey;
                        String itemValue = (String) itemData.get(itemName);
                        tmpTemplate.put("segment", itemName);
                        tmpTemplate.put("density", itemValue);
                        if (itemName.contains("INV")) {
                            dir = -1;
                            String[] parts = itemName.split("INV");
                            itemName = parts[0];
                            tmpTemplate.put("dir", dir);
                        }

                        String[] roadElements = itemName.split("--");
                        JSONArray roadElementsArray = new JSONArray();

                        for (int i = 0; i < roadElements.length; i++) {
                            roadElements[i] = roadElements[i] + "_" + dir; // serve per distinguere i roadelemet
                                                                           // percosrsi in senso invertito

                            if (i == roadElements.length - 1) {
                                // Dividi l'ultimo road element quando trovi "."
                                String[] lastElementParts = roadElements[i].split("\\.");
                                lastElementParts[0] = lastElementParts[0] + "_" + dir;
                                roadElements[i] = lastElementParts[0];
                            }

                            JSONObject tmpRoadElement = new JSONObject();
                            tmpRoadElement.put("roadElement", roadElements[i]);
                            roadElementsArray.put(tmpRoadElement);
                        }
                        tmpTemplate.put("roadElements", roadElementsArray);
                        JD20.put(tmpTemplate);
                    }
                }

            }

            return JD20;
        } catch (Exception e) {
            System.out.println("Error processing dinamic json: " + e);
            throw new Exception("Failed to send data to ES: " + e);
        }
    }

    private static JSONArray splitRoadElement(JSONArray jd20) throws Exception {
        JSONArray reArray = new JSONArray();
        try {
            for (int i = 0; i < jd20.length(); i++) {
                JSONObject jsonObject = jd20.getJSONObject(i);
                JSONArray roadElementsArray = jsonObject.getJSONArray("roadElements");

                for (int j = 0; j < roadElementsArray.length(); j++) {
                    JSONObject duplicatedObject = new JSONObject(jsonObject, JSONObject.getNames(jsonObject));
                    JSONArray tmp = new JSONArray();
                    JSONObject roadElementObject = roadElementsArray.getJSONObject(j);
                    String roadElement = roadElementObject.getString("roadElement");
                    tmp.put(roadElement);
                    duplicatedObject.put("roadElements", tmp);
                    reArray.put(duplicatedObject);
                }
            }
            return reArray;
        } catch (Exception e) {
            System.out.println("Error splitting the dynamic json: " + e);
            throw new Exception("Failed to send data to ES: " + e);
        }
    }

    private static Map<String, Double> mapDensity(JSONArray reArray) throws Exception {
        // Map "road element" con "density"
        Map<String, Double> densitySumMap = new HashMap<>();
        Map<String, Integer> countMap = new HashMap<>();
        Map<String, Double> densityAverageMap = new HashMap<>();

        try {
            for (int i = 0; i < reArray.length(); i++) {
                JSONObject jsonObject = reArray.getJSONObject(i);
                String roadElement = jsonObject.getJSONArray("roadElements").getString(0);
                double density = jsonObject.getDouble("density");

                // Aggiorna la somma delle densità per il road element
                if (densitySumMap.containsKey(roadElement)) {
                    double currentSum = densitySumMap.get(roadElement);
                    densitySumMap.put(roadElement, currentSum + density);
                } else {
                    densitySumMap.put(roadElement, density);
                }

                // Incrementa il contatore per il road element
                if (countMap.containsKey(roadElement)) {
                    int currentCount = countMap.get(roadElement);
                    countMap.put(roadElement, currentCount + 1);
                } else {
                    countMap.put(roadElement, 1);
                }
            }

            // Calcola la media per ciascun road element
            for (String roadElement : densitySumMap.keySet()) {
                double totalDensity = densitySumMap.get(roadElement);
                int count = countMap.get(roadElement);
                double averageDensity = totalDensity / count;
                densityAverageMap.put(roadElement, averageDensity);
            }

        } catch (Exception e) {
            System.out.println("Error mapping the density: " + e);
            throw new Exception("Failed to send data to ES: " + e);
        }
        ;

        return densityAverageMap;
    }

    private static JSONArray invertedIndex(JSONArray reArray, Map<String, Double> densityAverageMap) throws Exception {
        // Map per tenere traccia dei "road element" visitati
        Set<String> seenRoadElements = new HashSet<>();
        JSONArray invertedArray = new JSONArray();

        JSONArray filteredArray = new JSONArray();

        try {

            // filtro duplicati
            for (int i = 0; i < reArray.length(); i++) {
                JSONObject jsonObject = reArray.getJSONObject(i);
                String roadElement = jsonObject.getJSONArray("roadElements").getString(0);

                // verifica se road element è stato già visto
                if (!seenRoadElements.contains(roadElement)) {
                    seenRoadElements.add(roadElement);
                    filteredArray.put(jsonObject);
                }
            }

            // costruzione indice invertito
            Map<String, JSONArray> invertedMap = new HashMap<>();

            for (int i = 0; i < reArray.length(); i++) {
                JSONObject jsonObject = reArray.getJSONObject(i);
                String roadElement = jsonObject.getJSONArray("roadElements").getString(0);
                String segment = jsonObject.getString("segment");

                if (!invertedMap.containsKey(roadElement)) {
                    invertedMap.put(roadElement, new JSONArray());
                }

                invertedMap.get(roadElement).put(segment);

            }

            for (int i = 0; i < filteredArray.length(); i++) {
                JSONObject jsonObject = filteredArray.getJSONObject(i);

                JSONArray segments = invertedMap.get(jsonObject.getJSONArray("roadElements").getString(0));

                // Crea un nuovo array JSON per gli oggetti "roadElement"
                JSONArray newSegments = new JSONArray();

                // Aggiungi gli oggetti "roadElement" all'array nuovo
                for (int j = 0; j < segments.length(); j++) {
                    String roadElementValue = segments.getString(j);
                    JSONObject roadElementObject = new JSONObject();
                    roadElementObject.put("segment", roadElementValue);
                    newSegments.put(roadElementObject);
                }

                jsonObject.put("segments", newSegments);
                jsonObject.remove("segment");
                jsonObject.put("density",
                        (densityAverageMap.get(jsonObject.getJSONArray("roadElements").getString(0)) * 1000 / 20));
                String reName = jsonObject.getJSONArray("roadElements").getString(0);
                int index_ = reName.indexOf("_");
                jsonObject.put("roadElements", reName.substring(0, index_));
                invertedArray.put(jsonObject);
            }

        } catch (Exception e) {
            System.out.println("Error building the inverted index: " + e);
            throw new Exception("Failed to send data to ES: " + e);
        }

        return invertedArray;
    }

    private static class PostProcessRes {
        private JSONArray data;
        private double minlat;
        private double maxlat;
        private double minlong;
        private double maxlong;

        public PostProcessRes(JSONArray data, double minlat, double maxlat, double minlong, double maxlong) {
            this.data = data;
            this.minlat = minlat;
            this.maxlat = maxlat;
            this.minlong = minlong;
            this.maxlong = maxlong;
        }

        public JSONArray getPostPorcessData() {
            return data;
        }

        public double[] getMinMaxCoordinates() {
            double coord[] = new double[4];
            coord[0] = minlat;
            coord[1] = maxlat;
            coord[2] = minlong;
            coord[3] = maxlong;
            return coord;
        }
    }

    private static PostProcessRes postProcess(JSONArray invertedArray, JSONArray coordArray) throws Exception {
        try {

            // MODIFICA MARCO ////////////////////////

            double minLat = Double.POSITIVE_INFINITY;
            double minLong = Double.POSITIVE_INFINITY;
            double maxLat = -1 * Double.POSITIVE_INFINITY;
            double maxLong = -1 * Double.POSITIVE_INFINITY;

            ////////////////////////////////////////////////////////

            for (int i = 0; i < invertedArray.length(); i++) {
                String roadelementInverted = invertedArray.getJSONObject(i).getString("roadElements");
                boolean foundMatch = false;

                // Scorrere il coordArray per cercare una corrispondenza
                int j = 0;

                while (!foundMatch && j < coordArray.length()) {
                    String roadelementCoord = coordArray.getJSONObject(j).getJSONObject("id").getString("value");

                    if (roadelementInverted.equals(roadelementCoord)) {
                        foundMatch = true;
                    } else {
                        j++; // Incrementa j per passare all'elemento successivo in coordArray
                    }
                }

                if (!foundMatch) {
                    // System.out.println("Il road element: " +
                    // invertedArray.getJSONObject(i).getString("roadElements")
                    // + " non ha trovato corrispondenza in KB!!");
                    invertedArray.remove(i);
                    i--; // Decrementa l'indice per continuare la verifica con il nuovo elemento in

                    // questa posizione
                } else {

                    String slong = coordArray.getJSONObject(j).getJSONObject("slong").getString("value");
                    String slat = coordArray.getJSONObject(j).getJSONObject("slat").getString("value");
                    String elong = coordArray.getJSONObject(j).getJSONObject("elong").getString("value");
                    String elat = coordArray.getJSONObject(j).getJSONObject("elat").getString("value");

                    // MODIFICA MARCO ///////////////////////////////////////
                    double slatDouble = Double.valueOf(slat);
                    double slongDouble = Double.valueOf(slong);
                    double elatDouble = Double.valueOf(elat);
                    double elongDouble = Double.valueOf(elong);

                    if (slatDouble < minLat) {
                        minLat = slatDouble;
                    }
                    if (slatDouble > maxLat) {
                        maxLat = slatDouble;
                    }
                    if (slongDouble < minLong) {
                        minLong = slongDouble;
                    }
                    if (slongDouble > maxLong) {
                        maxLong = slongDouble;
                    }

                    if (elatDouble < minLat) {
                        minLat = elatDouble;
                    }
                    if (elatDouble > maxLat) {
                        maxLat = elatDouble;
                    }
                    if (elongDouble < minLong) {
                        minLong = elongDouble;
                    }
                    if (elongDouble > maxLong) {
                        maxLong = elongDouble;
                    }

                    ////////////////////////////////////////////////////////

                    invertedArray.getJSONObject(i).getJSONObject("start").getJSONObject("location").put("lon", slong);

                    invertedArray.getJSONObject(i).getJSONObject("start").getJSONObject("location").put("lat", slat);

                    invertedArray.getJSONObject(i).getJSONObject("end").getJSONObject("location").put("lon", elong);

                    invertedArray.getJSONObject(i).getJSONObject("end").getJSONObject("location").put("lat", elat);

                    String lineString = "{\"type\": \"LineString\",\"coordinates\": [[" + slong + ", " + slat + "], ["
                            + elong + ", " + elat + "]]}";

                    JSONObject line = new JSONObject(lineString);

                    invertedArray.getJSONObject(i).put("line", line);

                }

            }
            PostProcessRes res = new PostProcessRes(invertedArray, minLat, maxLat, minLong, maxLong);
            return res;
        } catch (Exception e) {
            System.out.println("Error in post processing: " + e);
            throw new Exception("Failed to send data to ES: " + e);
        }
        // return invertedArray;
    }
    // ############################################# QUERY METHODS

    // thread per la richiesta delle coordinate dei road element
    static class KBThread extends Thread {
        private String query;
        private String sparqlEndpoint;
        private CloseableHttpClient httpClient;
        private JSONArray allResults;
        private int index;
        private static boolean errorFlag = false;
        private static String errorMessage;

        public KBThread(int index, String query, String sparqlEndpoint, JSONArray allResults,
                CloseableHttpClient httpClient) {
            this.query = query;
            this.allResults = allResults;
            this.sparqlEndpoint = sparqlEndpoint;
            this.httpClient = httpClient;
            this.index = index;
        }

        @Override
        public void run() {
            try {

                String encodedQuery;
                JSONArray resultsArray = null;
                try {
                    encodedQuery = URLEncoder.encode(query, "UTF-8");
                    HttpGet httpGet = new HttpGet(sparqlEndpoint + "&query=" + encodedQuery);
                    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                        if (response.getStatusLine().getStatusCode() != 200) {
                            System.out.println("Error: " + response);
                        }
                        ;
                        String resp = EntityUtils.toString(response.getEntity());
                        JSONObject jsonResp = new JSONObject(resp);
                        resultsArray = jsonResp.getJSONObject("results").getJSONArray("bindings");
                    }

                    // System.out.println("risultato thread " + resultsArray);
                } catch (Exception e) {
                    System.out.println("Error retrieving road element coordinates in KB, thread"
                            + index + ": " + e);
                    errorFlag = true;
                    errorMessage = e.getMessage();
                }
                synchronized (allResults) {
                    // System.out.println("thread" + index + " " + resultsArray.get(0));
                    for (int i = 0; i < resultsArray.length(); i++) {
                        allResults.put(resultsArray.getJSONObject(i));
                    }
                }

            } catch (Exception ex) {
            }
        }

        public static boolean hasErrorOccurred() {
            return errorFlag;
        }

        public static String errorMessage() {
            return errorMessage;
        }
    }

    private static JSONObject getCoord(JSONArray invertedArray, int batchSize, String kbUrl,
            CloseableHttpClient httpClient) throws Exception {

        JSONObject totalJsonResp = new JSONObject(); // Oggetto JSON che conterrà tutti i risultati

        try {
            List<String> seenRoadElement = new ArrayList<>();

            for (int i = 0; i < invertedArray.length(); i++) {
                JSONObject jsonObject = invertedArray.getJSONObject(i);
                String roadElement = jsonObject.getString("roadElements");

                if (!seenRoadElement.contains(roadElement)) {
                    seenRoadElement.add(roadElement);
                }
            }

            String sparqlEndpoint = kbUrl;
            int totalElements = seenRoadElement.size();
            List<String> queryList = new ArrayList<>();

            JSONArray allResults = new JSONArray(); // Array per accumulare tutti i risultati

            for (int start = 0; start < totalElements; start += batchSize) {
                int end = Math.min(start + batchSize, totalElements);
                StringBuilder filter = new StringBuilder();

                for (int i = start; i < end; i++) {
                    filter.append("?id=\"").append(seenRoadElement.get(i)).append("\"");
                    if (i < end - 1) {
                        filter.append(" || ");
                    }
                }

                String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>\n" +
                        "PREFIX dct: <http://purl.org/dc/terms/>\n" +
                        "SELECT   (xsd:string(?id) as ?id) (xsd:string(?slong) as ?slong) (xsd:string(?slat) as ?slat) (xsd:string(?elong) as ?elong) (xsd:string(?elat) as ?elat)\n"
                        +
                        "WHERE {\n" +
                        "  ?s a km4c:RoadElement.\n" +
                        "  ?s dct:identifier ?id.\n" +
                        "  ?s km4c:startsAtNode ?ns.\n" +
                        "  ?s km4c:endsAtNode ?ne.\n" +
                        "  ?ns geo:long ?slong.\n" +
                        "  ?ns geo:lat ?slat.\n" +
                        "  ?ne geo:long ?elong.\n" +
                        "  ?ne geo:lat ?elat.\n" +
                        "  FILTER (" + filter.toString() + ").\n" +
                        "}";
                queryList.add(queryString);
            }

            List<Thread> threads = new ArrayList<>();
            int index = 0;

            for (String query : queryList) {
                Thread KBThread = new KBThread(index, query, sparqlEndpoint, allResults, httpClient);
                index++;
                threads.add(KBThread);
                KBThread.start();
            }

            // Attendere il completamento di tutti i thread
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("In KB, has error occurred: " + KBThread.hasErrorOccurred());

            if (KBThread.hasErrorOccurred()) {
                throw new Exception("Failed to send data to ES: " + KBThread.errorMessage());
            }

            // Aggiungi l'array di tutti i risultati all'oggetto JSON di risposta
            totalJsonResp.put("results", allResults);

        } catch (Exception e) {
            System.out.println(e);
            throw new Exception("Failed to send data to ES: " + KBThread.errorMessage());
        }
        return totalJsonResp;
    }

    // thread per inserimenti in ES
    static class ESThread extends Thread {
        private int index;
        private JSONArray documents;
        private String indexName;
        private String url;
        private static boolean errorFlag = false;
        private static String errorMessage;
        private int maxErrors;
        private ErrorCounter errorCounter;
        private RestClientBuilder builder;

        public ESThread(int index, String indexName, JSONArray documents, String url, int maxErrors,
                ErrorCounter errorCounter, RestClientBuilder builder) {
            this.index = index;
            this.documents = documents;
            this.indexName = indexName;
            this.url = url;
            this.maxErrors = maxErrors;
            this.errorCounter = errorCounter;
            this.builder = builder;
        }

        @Override
        public void run() {
            try {

                boolean status = sendDoc(indexName, documents, url, maxErrors, errorCounter,
                        builder);

                if (!status) {
                    System.out.println("Failed to send data to ES: too many errors indexing data");
                    throw new Exception("Failed to send data to ES");
                }

            } catch (Exception e) {
                System.out.println("Error indexing document in Es, thread" + index + ": " + e);
                errorFlag = true;
                errorMessage = e.getMessage();
            }
        }

        public static boolean hasErrorOccurred() {
            return errorFlag;
        }

        public static String errorMessage() {
            return errorMessage;
        }
    }

    private static void sendToIndex(int threadNumber, JSONArray invertedArray, String indexName, String[] url, int port,
            String admin, String password, int maxErrors, ErrorCounter errorCounter, RestClientBuilder builder)
            throws Exception {

        try {
            int length = invertedArray.length();
            int numParts = Math.min(threadNumber, length); // Ensure that the number of parts is not greater than the
                                                           // length of the array
            int chunkSize = (int) Math.ceil((double) length / numParts); // Calculate the size of each chunk, rounding
                                                                         // up to handle any remainder
            JSONArray[] dividedArrays = new JSONArray[numParts]; // Create an array of JSONArray

            for (int i = 0; i < numParts; i++) {
                dividedArrays[i] = new JSONArray(); // Create a new JSONArray for each part
            }

            for (int i = 0; i < length; i++) {
                int chunkIndex = Math.min(i / chunkSize, numParts - 1); // Calculate the index of the part to which to
                                                                        // add the element
                dividedArrays[chunkIndex].put(invertedArray.get(i)); // Add the element to the corresponding JSONArray
            }

            List<Thread> threads = new ArrayList<>();

            for (int i = 0; i < numParts; i++) {
                Thread ESThread = new ESThread(i, indexName, dividedArrays[i], url[i % url.length], maxErrors,
                        errorCounter, builder);
                threads.add(ESThread);
                ESThread.start();
            }

            // Attendere il completamento di tutti i thread
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (ESThread.hasErrorOccurred()) {
                throw new Exception("Failed to send data to ES: " + ESThread.errorMessage());
            }
        } catch (Exception e) {
            System.out.println("Error sending data to ES: " + e.getMessage());
            throw new Exception("Failed to send data to ES: " + ESThread.errorMessage());
        }

    }

    private static boolean sendDoc(String indexName, JSONArray jsonArray, String url, int maxErrors,
            ErrorCounter errorCounter, RestClientBuilder builder)
            throws Exception {

        RestHighLevelClient client = new RestHighLevelClient(builder);
        // Inizializza il client Elasticsearch

        int nSent = 0;

        String jsonDocument = "";
        IndexResponse response = null;

        // Invia il documento JSON a Elasticsearch per l'indicizzazione

        for (int i = 0; i < jsonArray.length(); i++) {
            jsonDocument = jsonArray.getJSONObject(i).toString();
            try {
                if (errorCounter.getErrorCount() < maxErrors) {
                    IndexRequest request = new IndexRequest(indexName).source(jsonDocument,
                            XContentType.JSON);
                    client.index(request, RequestOptions.DEFAULT);
                    nSent++;
                }
            } catch (Exception e) {
                System.err.println("Error indexing document: " + e);
                Thread.sleep(500);
                errorCounter.incrementErrorCount();
                System.out.println(errorCounter.getErrorCount());
                System.out.println("Retrying for the current document");
                // Riprova solo per l'elemento corrente
                try {
                    IndexRequest retryRequest = new IndexRequest(indexName).source(jsonDocument,
                            XContentType.JSON);
                    client.index(retryRequest, RequestOptions.DEFAULT);
                    nSent++;
                    System.out.println("New indexing attempt successful");
                } catch (Exception ex) {
                    System.err.println("Error twice indexing document the document has not been indexed: " + ex);
                }
            }
        }
        client.close();
        System.out.println(Thread.currentThread().getName() + " sent:" + nSent + " hostname:" + url);
        if (errorCounter.getErrorCount() < maxErrors) {
            return true;
        } else {
            return false;
        }

    }

    // #################################### INDEX CREATION

    private static void createIndex(String indexName, String url, int port, String admin, String password)
            throws Exception {
        try {
            // Specifica le credenziali di accesso
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(admin, password));

            RestClientBuilder builder = RestClient.builder(
                    new HttpHost(url, port, "https"))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            // Configura l'autenticazione HTTP
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            return httpClientBuilder;
                        }
                    });
            System.out.println("creato il builder");

            RestHighLevelClient client = new RestHighLevelClient(builder);

            System.out.println("creato il client");

            Settings settings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
                    .build();

            System.out.println("creato il settings");

            CreateIndexRequest request = new CreateIndexRequest(indexName)
                    .settings(settings)
                    .mapping(indexMapping());

            System.out.println("creato il request");

            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println("response id: " + response.index());
            client.close();

            System.out.println("creato il response");

        } catch (Exception e) {
            System.out.println("Eccezione nella creazione dell'indice: " + e);
            throw new Exception("Failed to send data to ES: " + e);

        }
    }

    private static XContentBuilder indexMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("properties");
            {
                mapping.startObject("dateObserved");
                {
                    mapping.field("type", "date");
                    mapping.field("format", "yyyy-MM-dd'T'HH:mm:ss");
                }
                mapping.endObject();

                mapping.startObject("density");
                {
                    mapping.field("type", "float");
                }
                mapping.endObject();

                mapping.startObject("scenario");
                {
                    mapping.field("type", "keyword");
                }
                mapping.endObject();

                mapping.startObject("kind");
                {
                    mapping.field("type", "keyword");
                }
                mapping.endObject();

                mapping.startObject("numVehicle");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();

                mapping.startObject("roadElements");
                {
                    mapping.field("type", "keyword");
                }
                mapping.endObject();

                mapping.startObject("segments");
                {
                    mapping.field("type", "nested");
                    mapping.startObject("properties");
                    {
                        mapping.startObject("segment");
                        {
                            mapping.field("type", "keyword");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();

                mapping.startObject("start");
                {
                    mapping.startObject("properties");
                    {
                        mapping.startObject("location");
                        {
                            mapping.field("type", "geo_point");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();

                mapping.startObject("end");
                {
                    mapping.startObject("properties");
                    {
                        mapping.startObject("location");
                        {
                            mapping.field("type", "geo_point");
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();

                mapping.startObject("dir");
                {
                    mapping.field("type", "integer");
                }
                mapping.endObject();

                mapping.startObject("flow");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();

                mapping.startObject("line");
                {
                    mapping.field("type", "geo_shape");
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        return mapping;
    }

}
