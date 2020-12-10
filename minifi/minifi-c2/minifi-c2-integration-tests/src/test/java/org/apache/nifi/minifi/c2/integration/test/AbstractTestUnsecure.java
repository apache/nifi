/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
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

package org.apache.nifi.minifi.c2.integration.test;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public abstract class AbstractTestUnsecure {
    protected String c2Url;

    public static String getUnsecureConfigUrl(Container container) {
        DockerPort dockerPort = container.port(10080);
        return "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort() + "/c2/config";
    }

    protected void setup(DockerComposeRule docker) {
        c2Url = getConfigUrl(docker);
    }

    protected String getConfigUrl(DockerComposeRule docker) {
        return getUnsecureConfigUrl(docker.containers().container("c2"));
    }

    @Test
    public void testCurrentVersion() throws IOException, SchemaLoaderException {
        ConfigSchema configSchema = getConfigSchema(c2Url + "?class=raspi3");
        assertEquals(3, configSchema.getVersion());
        assertEquals("raspi3.v2", configSchema.getFlowControllerProperties().getName());
    }

    @Test
    public void testVersion1() throws IOException, SchemaLoaderException {
        ConfigSchema configSchema = getConfigSchema(c2Url + "?class=raspi3&version=1");
        assertEquals(3, configSchema.getVersion());
        assertEquals("raspi3.v1", configSchema.getFlowControllerProperties().getName());
    }

    @Test
    public void testVersion2() throws IOException, SchemaLoaderException {
        ConfigSchema configSchema = getConfigSchema(c2Url + "?class=raspi3&version=2");
        assertEquals(3, configSchema.getVersion());
        assertEquals("raspi3.v2", configSchema.getFlowControllerProperties().getName());
    }

    @Test
    public void testUnacceptable() throws IOException {
        HttpURLConnection urlConnection = openSuperUserUrlConnection(c2Url + "?class=raspi3");
        try {
            urlConnection.setRequestProperty("Accept", "text/xml");
            assertEquals(406, urlConnection.getResponseCode());
        } finally {
            urlConnection.disconnect();
        }
    }

    @Test
    public void testInvalid() throws IOException {
        HttpURLConnection urlConnection = openSuperUserUrlConnection(c2Url);
        try {
            assertEquals(400, urlConnection.getResponseCode());
        } finally {
            urlConnection.disconnect();
        }
    }

    public ConfigSchema getConfigSchema(String urlString) throws IOException, SchemaLoaderException {
        HttpURLConnection urlConnection = openSuperUserUrlConnection(urlString);
        ConfigSchema configSchema;
        try (InputStream inputStream = urlConnection.getInputStream()) {
            configSchema = SchemaLoader.loadConfigSchemaFromYaml(inputStream);
        } finally {
            urlConnection.disconnect();
        }
        return configSchema;
    }

    protected HttpURLConnection openSuperUserUrlConnection(String url) throws IOException {
        return (HttpURLConnection) new URL(url).openConnection();
    }
}
