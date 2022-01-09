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

package org.apache.nifi.minifi.bootstrap.configuration.ingestors.common;

import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ListenerHandleResult;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.interfaces.Differentiator;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor;
import org.apache.nifi.minifi.bootstrap.util.ByteBufferInputStream;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.common.ConvertableSchema;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.apache.nifi.util.file.FileUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;

import static org.apache.nifi.minifi.bootstrap.configuration.ingestors.PullHttpChangeIngestor.PATH_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class PullHttpChangeIngestorCommonTest {

    public static volatile Server jetty;
    public static volatile int port;
    public static volatile PullHttpChangeIngestor pullHttpChangeIngestor;
    public static ConfigurationChangeNotifier testNotifier = Mockito.mock(ConfigurationChangeNotifier.class);
    public static Differentiator<ByteBuffer> mockDifferentiator = Mockito.mock(Differentiator.class);
    public static final String RESPONSE_STRING = "test";
    public static final String PATH_RESPONSE_STRING = "path";
    public static ByteBuffer configBuffer= ByteBuffer.wrap(RESPONSE_STRING.getBytes());
    public static ByteBuffer pathConfigBuffer= ByteBuffer.wrap(PATH_RESPONSE_STRING.getBytes());
    public static final String ETAG = "testEtag";
    public static final String QUOTED_ETAG = "\"testEtag\"";

    public static void init() {
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool();
        queuedThreadPool.setDaemon(true);
        jetty = new Server(queuedThreadPool);

        HandlerCollection handlerCollection = new HandlerCollection(true);
        handlerCollection.addHandler(new JettyHandler(RESPONSE_STRING, PATH_RESPONSE_STRING));
        jetty.setHandler(handlerCollection);
    }

    public abstract void pullHttpChangeIngestorInit(Properties properties);

    @Before
    public void before() {
        Mockito.reset(testNotifier);
        ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        when(testListener.getDescriptor()).thenReturn("MockChangeListener");
        Mockito.when(testNotifier.notifyListeners(Mockito.any())).thenReturn(Collections.singleton(new ListenerHandleResult(testListener)));
    }

    @AfterClass
    public static void shutdown() throws Exception {
        jetty.stop();
    }

    @Test
    public void testNewUpdate() throws IOException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        pullHttpChangeIngestorInit(properties);
        pullHttpChangeIngestor.setUseEtag(false);
        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(true);

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.eq(configBuffer.asReadOnlyBuffer()));
    }

    @Test
    public void testSecurityOverride() throws IOException, SchemaLoaderException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "false");
        properties.put(RunMiNiFi.MINIFI_CONFIG_FILE_KEY, "src/test/resources/config.yml");
        properties.put(PATH_KEY, "/config-minimal.yml");
        pullHttpChangeIngestorInit(properties);
        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(true);

        pullHttpChangeIngestor.run();

        ArgumentCaptor<ByteBuffer> argument = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(testNotifier, Mockito.times(1)).notifyListeners(argument.capture());

        ConvertableSchema<ConfigSchema> configSchema = SchemaLoader.loadConvertableSchemaFromYaml(new ByteBufferInputStream(argument.getValue()));
        ConfigSchema newSchema = configSchema.convert();

        assertNotNull(newSchema.getSecurityProperties().getKeystore());
        assertEquals(newSchema.getProcessGroupSchema().getProcessors().size(), 2);
    }

    @Test
    public void testNoUpdate() throws IOException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        pullHttpChangeIngestorInit(properties);
        pullHttpChangeIngestor.setUseEtag(false);
        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(false);

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.never()).notifyListeners(Mockito.any());
    }

    @Test
    public void testUseEtag() throws IOException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        pullHttpChangeIngestorInit(properties);
        pullHttpChangeIngestor.setLastEtag("");

        pullHttpChangeIngestor.setUseEtag(true);

        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(true);

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.eq(configBuffer));

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.any());

    }

    @Test
    public void testNewUpdateWithPath() throws IOException {
        Properties properties = new Properties();
        properties.put(PATH_KEY, "/config.yml");
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        pullHttpChangeIngestorInit(properties);
        pullHttpChangeIngestor.setUseEtag(false);
        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(true);

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.eq(pathConfigBuffer.asReadOnlyBuffer()));
    }

    @Test
    public void testNoUpdateWithPath() throws IOException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        properties.put(PATH_KEY, "/config.yml");
        pullHttpChangeIngestorInit(properties);
        pullHttpChangeIngestor.setUseEtag(false);
        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(false);

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.never()).notifyListeners(Mockito.any());
    }

    @Test
    public void testUseEtagWithPath() throws IOException {
        Properties properties = new Properties();
        properties.put(PullHttpChangeIngestor.OVERRIDE_SECURITY, "true");
        properties.put(PATH_KEY, "/config.yml");
        pullHttpChangeIngestorInit(properties);
        pullHttpChangeIngestor.setLastEtag("");

        pullHttpChangeIngestor.setUseEtag(true);

        when(mockDifferentiator.isNew(Mockito.any(ByteBuffer.class))).thenReturn(true);

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.eq(pathConfigBuffer.asReadOnlyBuffer()));

        pullHttpChangeIngestor.run();

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.any());

    }

    static class JettyHandler extends AbstractHandler {
        volatile String configResponse;
        volatile String pathResponse;

        public JettyHandler(String configResponse, String pathResponse){
            this.configResponse = configResponse;
            this.pathResponse = pathResponse;
        }

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {

            baseRequest.setHandled(true);

            if ("GET".equals(request.getMethod())) {

                if (QUOTED_ETAG.equals(baseRequest.getHeader("If-None-Match"))){
                    writeOutput(response, null, 304);
                } else {
                    if ("/config.yml".equals(baseRequest.getPathInfo())) {
                        writeOutput(response, pathResponse, 200);
                    } else if ("/config-minimal.yml".equals(baseRequest.getPathInfo())) {
                        writeFileOutput(response, new File("src/test/resources/config-minimal.yml"), 200);
                    } else {
                        writeOutput(response, configResponse, 200);
                    }
                }


            } else {
                writeOutput(response, "not a GET request", 404);
            }
        }

        private void writeOutput(HttpServletResponse response, String responseBuffer, int responseCode) throws IOException {
            response.setStatus(responseCode);
            response.setHeader("ETag", ETAG);
            if (responseBuffer != null) {
                response.setContentType("text/plain");
                response.setContentLength(responseBuffer.length());
                response.setCharacterEncoding(StandardCharsets.UTF_8.displayName());
                try (PrintWriter writer = response.getWriter()) {
                    writer.print(responseBuffer);
                    writer.flush();
                }
            }
        }

        private void writeFileOutput(HttpServletResponse response, File file, int responseCode) throws IOException {
            response.setStatus(responseCode);
            response.setHeader("ETag", ETAG);
            if (file != null) {
                response.setContentType("text/plain");
                response.setContentLength((int) file.length());
                response.setCharacterEncoding(StandardCharsets.UTF_8.displayName());
                FileUtils.copyFile(file, response.getOutputStream(), true, true);
            }
        }

    }
}
