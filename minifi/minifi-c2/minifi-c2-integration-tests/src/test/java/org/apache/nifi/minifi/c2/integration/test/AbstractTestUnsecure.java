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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.junit.jupiter.api.Test;

public abstract class AbstractTestUnsecure {
    protected String c2Url;

    public static String getUnsecureConfigUrl(Container container) {
        DockerPort dockerPort = container.port(10090);
        return "http://" + dockerPort.getIp() + ":" + dockerPort.getExternalPort() + "/c2/config";
    }

    protected void setup(DockerComposeExtension docker) {
        c2Url = getConfigUrl(docker);
    }

    protected String getConfigUrl(DockerComposeExtension docker) {
        return getUnsecureConfigUrl(docker.containers().container("c2"));
    }

    @Test
    public void testCurrentVersion() throws IOException {
        VersionedDataflow versionedDataflow = getFlowDefinition(c2Url + "?class=raspi3");
        assertEquals("raspi3.v2", versionedDataflow.getRootGroup().getName());
    }

    @Test
    public void testVersion1() throws IOException {
        VersionedDataflow versionedDataflow = getFlowDefinition(c2Url + "?class=raspi3&version=1");
        assertEquals("raspi3.v1", versionedDataflow.getRootGroup().getName());
    }

    @Test
    public void testVersion2() throws IOException {
        VersionedDataflow versionedDataflow = getFlowDefinition(c2Url + "?class=raspi3&version=2");
        assertEquals("raspi3.v2", versionedDataflow.getRootGroup().getName());
    }

    @Test
    public void testUnacceptable() throws IOException {
        HttpURLConnection urlConnection = openSuperUserUrlConnection(c2Url + "?class=raspi3");
        try {
            urlConnection.setRequestProperty("Accept", "text/invalid");
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

    public VersionedDataflow getFlowDefinition(String urlString) throws IOException {
        HttpURLConnection urlConnection = openSuperUserUrlConnection(urlString);
        urlConnection.setRequestProperty("Accept", "application/json");
        try (InputStream inputStream = urlConnection.getInputStream()) {
            return toVersionedDataFlow(inputStream);
        } finally {
            urlConnection.disconnect();
        }
    }

    protected HttpURLConnection openSuperUserUrlConnection(String url) throws IOException {
        return (HttpURLConnection) URI.create(url).toURL().openConnection();
    }

    protected VersionedDataflow toVersionedDataFlow(InputStream inputStream) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.readValue(IOUtils.toByteArray(inputStream), VersionedDataflow.class);
    }
}
