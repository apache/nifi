/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.elasticsearch.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.http.auth.AuthScope.ANY;

public abstract class AbstractElasticsearch_IT {
    protected static final DockerImageName IMAGE = DockerImageName
            .parse(System.getProperty("elasticsearch.docker.image"));
    protected static final ElasticsearchContainer ELASTICSEARCH_CONTAINER = new ElasticsearchContainer(IMAGE)
            .withPassword(System.getProperty("elasticsearch.elastic_user.password"))
            .withEnv("xpack.security.enabled", "true");

    protected static final String ELASTIC_USER_PASSWORD = System.getProperty("elasticsearch.elastic_user.password");
    protected static final boolean ENABLE_TEST_CONTAINERS = System.getProperty("elasticsearch.testcontainers.enabled")
            != null && System.getProperty("elasticsearch.testcontainers.enabled").equalsIgnoreCase("true");
    protected static String ELASTIC_HOST;

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    public static void startTestcontainer() {
        if (ENABLE_TEST_CONTAINERS) {
            ELASTICSEARCH_CONTAINER.start();
            ELASTIC_HOST = String.format("http://%s", ELASTICSEARCH_CONTAINER.getHttpHostAddress());
        } else {
            ELASTIC_HOST = System.getProperty("elasticsearch.endpoint");
        }
    }

    public static void stopTestcontainer() {
        if (ENABLE_TEST_CONTAINERS) {
            ELASTICSEARCH_CONTAINER.stop();
        }
    }

    private static String[] getElasticVersion() {
        String fullVersion = IMAGE.getVersionPart();
        String[] parts = fullVersion.split("\\.");
        if (parts.length == 1) {
            throw new RuntimeException("The elasticsearch version should have at least a major and minor version ex. 7.17");
        }

        return parts;
    }

    protected static int getElasticMajorVersion() {
        return Integer.valueOf(getElasticVersion()[0]);
    }

    protected static int getElasticMinorVersion() {
        return Integer.valueOf(getElasticVersion()[1]);
    }

    private static RestClient testDataManagementClient;

    protected static void setupTestData() throws IOException {
        int majorVersion = getElasticMajorVersion();
        URL url = new URL(ELASTIC_HOST);
        testDataManagementClient = RestClient
                .builder(new HttpHost(url.getHost(), url.getPort(), url.getProtocol()))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("elastic", ELASTIC_USER_PASSWORD);
                    BasicCredentialsProvider provider = new BasicCredentialsProvider();
                    provider.setCredentials(ANY, credentials);
                    httpClientBuilder.setDefaultCredentialsProvider(provider);
                    return httpClientBuilder;
                })
                .build();
        String script = String.format("src/test/resources/setup-%s.script", majorVersion);

        List<SetupAction> actions = readSetupActions(script);

        for (SetupAction action : actions) {
            String endpoint = String.format("%s/%s", ELASTIC_HOST, action.path);
            Request request = new Request(action.verb, endpoint);
            HttpEntity jsonBody = new NStringEntity(action.json, ContentType.APPLICATION_JSON);
            request.setEntity(jsonBody);

            try {
                testDataManagementClient.performRequest(request);
            } catch (ResponseException re) {
                throw new RuntimeException(re);
            }
        }
    }

    protected static void tearDownTestData() throws IOException {
        deleteIndex("user_details");
        deleteIndex("complex");
        deleteIndex("nested");
        deleteIndex("bulk_a");
        deleteIndex("bulk_b");
        deleteIndex("bulk_c");
        deleteIndex("error_handler");
        deleteIndex("messages");
    }

    private static void deleteIndex(String name) throws IOException {
        Request request = new Request("DELETE", String.format("%s/%s", ELASTIC_HOST, name));
        testDataManagementClient.performRequest(request);
    }

    private static List<SetupAction> readSetupActions(String scriptPath) throws IOException {
        List<SetupAction> actions = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(scriptPath)));
        String line = reader.readLine();
        while (line != null) {
            if (!line.trim().isEmpty() && !line.trim().startsWith("#")) {
                String verb = line.substring(0, line.indexOf(":"));
                String path = line.substring(verb.length() + 1, line.indexOf(":", verb.length() + 1));
                int loc = verb.length() + path.length() + 2;
                String json = line.substring(loc);

                actions.add(new SetupAction(verb, path, json));
            }
            line = reader.readLine();
        }

        return actions;
    }

    private static final class SetupAction {
        private String verb;
        private String path;
        private String json;

        public SetupAction(String verb, String path, String json) {
            this.verb = verb;
            this.path = path;
            this.json = json;
        }
    }
}
