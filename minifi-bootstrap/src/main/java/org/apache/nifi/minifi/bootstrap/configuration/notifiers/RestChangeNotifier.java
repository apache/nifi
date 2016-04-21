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

package org.apache.nifi.minifi.bootstrap.configuration.notifiers;

import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ListenerHandleResult;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.nifi.minifi.bootstrap.RunMiNiFi.NOTIFIER_PROPERTY_PREFIX;


public class RestChangeNotifier implements ConfigurationChangeNotifier {

    private final Set<ConfigurationChangeListener> configurationChangeListeners = new HashSet<>();
    private final static Logger logger = LoggerFactory.getLogger(RestChangeNotifier.class);
    private String configFile = null;
    private final Server jetty;
    public static final String GET_TEXT = "This is a config change listener for an Apache NiFi - MiNiFi instance.\n" +
            "Use this rest server to upload a conf.yml to configure the MiNiFi instance.\n" +
            "Send a POST http request to '/' to upload the file.";
    public static final String OTHER_TEXT ="This is not a support HTTP operation. Please use GET to get more information or POST to upload a new config.yml file.\n";


    public static final String POST = "POST";
    public static final String GET = "GET";

    public static final String PORT_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.port";
    public static final String HOST_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.host";
    public static final String TRUSTSTORE_LOCATION_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.truststore.location";
    public static final String TRUSTSTORE_PASSWORD_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.truststore.password";
    public static final String TRUSTSTORE_TYPE_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.truststore.type";
    public static final String KEYSTORE_LOCATION_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.keystore.location";
    public static final String KEYSTORE_PASSWORD_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.keystore.password";
    public static final String KEYSTORE_TYPE_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.keystore.type";
    public static final String NEED_CLIENT_AUTH_KEY = NOTIFIER_PROPERTY_PREFIX + ".http.need.client.auth";

    public RestChangeNotifier(){
        QueuedThreadPool queuedThreadPool = new QueuedThreadPool();
        queuedThreadPool.setDaemon(true);
        jetty = new Server(queuedThreadPool);
    }

    @Override
    public void initialize(Properties properties) {
        logger.info("Initializing");

        // create the secure connector if keystore location is specified
        if (properties.getProperty(KEYSTORE_LOCATION_KEY) != null) {
            createSecureConnector(properties);
        } else {
            // create the unsecure connector otherwise
            createConnector(properties);
        }

        HandlerCollection handlerCollection = new HandlerCollection(true);
        handlerCollection.addHandler(new JettyHandler());
        jetty.setHandler(handlerCollection);
    }

    @Override
    public Set<ConfigurationChangeListener> getChangeListeners() {
        return configurationChangeListeners;
    }

    @Override
    public boolean registerListener(ConfigurationChangeListener listener) {
        return configurationChangeListeners.add(listener);
    }

    @Override
    public Collection<ListenerHandleResult> notifyListeners() {
        if (configFile == null){
            throw new IllegalStateException("Attempting to notify listeners when there is no new config file.");
        }

        Collection<ListenerHandleResult> listenerHandleResults = new ArrayList<>(configurationChangeListeners.size());
        for (final ConfigurationChangeListener listener : getChangeListeners()) {
            ListenerHandleResult result;
            try (final ByteArrayInputStream fis = new ByteArrayInputStream(configFile.getBytes())) {
                listener.handleChange(fis);
                result = new ListenerHandleResult(listener);
            } catch (IOException | ConfigurationChangeException ex) {
                result = new ListenerHandleResult(listener, ex);
            }
            listenerHandleResults.add(result);
            logger.info("Listener notification result:" + result.toString());
        }

        configFile = null;
        return listenerHandleResults;
    }

    @Override
    public void start(){
        try {
            jetty.start();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public void close() throws IOException {
        logger.warn("Shutting down the jetty server");
        try {
            jetty.stop();
            jetty.destroy();
        } catch (Exception e) {
            throw new IOException(e);
        }
        logger.warn("Done shutting down the jetty server");
    }

    public URI getURI(){
        return jetty.getURI();
    }

    public int getPort(){
        if (!jetty.isStarted()) {
            throw new IllegalStateException("Jetty server not started");
        }
        return ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    public String getConfigString(){
        return configFile;
    }

    private void setConfigFile(String configFile){
        this.configFile = configFile;
    }

    private void createConnector(Properties properties) {
        final ServerConnector http = new ServerConnector(jetty);

        http.setPort(Integer.parseInt(properties.getProperty(PORT_KEY, "0")));
        http.setHost(properties.getProperty(HOST_KEY, "localhost"));

        // Severely taxed or distant environments may have significant delays when executing.
        http.setIdleTimeout(30000L);
        jetty.addConnector(http);

        logger.info("Added an http connector on the host '{}' and port '{}'", new Object[]{http.getHost(), http.getPort()});
    }

    private void createSecureConnector(Properties properties) {
        SslContextFactory ssl = new SslContextFactory();

        if (properties.getProperty(KEYSTORE_LOCATION_KEY) != null) {
            ssl.setKeyStorePath(properties.getProperty(KEYSTORE_LOCATION_KEY));
            ssl.setKeyStorePassword(properties.getProperty(KEYSTORE_PASSWORD_KEY));
            ssl.setKeyStoreType(properties.getProperty(KEYSTORE_TYPE_KEY));
        }

        if (properties.getProperty(TRUSTSTORE_LOCATION_KEY) != null) {
            ssl.setTrustStorePath(properties.getProperty(TRUSTSTORE_LOCATION_KEY));
            ssl.setTrustStorePassword(properties.getProperty(TRUSTSTORE_PASSWORD_KEY));
            ssl.setTrustStoreType(properties.getProperty(TRUSTSTORE_TYPE_KEY));
            ssl.setNeedClientAuth(Boolean.parseBoolean(properties.getProperty(NEED_CLIENT_AUTH_KEY, "true")));
        }

        // build the connector
        final ServerConnector https = new ServerConnector(jetty, ssl);

        // set host and port
        https.setPort(Integer.parseInt(properties.getProperty(PORT_KEY,"0")));
        https.setHost(properties.getProperty(HOST_KEY, "localhost"));

        // Severely taxed environments may have significant delays when executing.
        https.setIdleTimeout(30000L);

        // add the connector
        jetty.addConnector(https);

        logger.info("Added an https connector on the host '{}' and port '{}'", new Object[]{https.getHost(), https.getPort()});
    }


    public class JettyHandler extends AbstractHandler {

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {

            logRequest(request);

            baseRequest.setHandled(true);

            if(POST.equals(request.getMethod())) {
                final StringBuilder configBuilder = new StringBuilder();
                BufferedReader reader = request.getReader();
                if(reader != null && reader.ready()){
                    String line;
                    while ((line = reader.readLine()) != null) {
                        configBuilder.append(line);
                        configBuilder.append(System.getProperty("line.separator"));
                    }
                }
                setConfigFile(configBuilder.substring(0,configBuilder.length()-1));
                Collection<ListenerHandleResult> listenerHandleResults = notifyListeners();

                int statusCode = 200;
                for (ListenerHandleResult result: listenerHandleResults){
                    if(!result.succeeded()){
                        statusCode = 500;
                        break;
                    }
                }

                writeOutput(response, getPostText(listenerHandleResults), statusCode);
            } else if(GET.equals(request.getMethod())) {
                writeOutput(response, GET_TEXT, 200);
            } else {
                writeOutput(response, OTHER_TEXT, 404);
            }
        }

        private String getPostText(Collection<ListenerHandleResult> listenerHandleResults){
            StringBuilder postResult = new StringBuilder("The result of notifying listeners:\n");

            for (ListenerHandleResult result : listenerHandleResults) {
                postResult.append(result.toString());
                postResult.append("\n");
            }

            return postResult.toString();
        }

        private void writeOutput(HttpServletResponse response, String responseText, int responseCode) throws IOException {
            response.setStatus(responseCode);
            response.setContentType("text/plain");
            response.setContentLength(responseText.length());
            try (PrintWriter writer = response.getWriter()) {
                writer.print(responseText);
                writer.flush();
            }
        }

        private void logRequest(HttpServletRequest request){
            logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            logger.info("request method = " + request.getMethod());
            logger.info("request url = " + request.getRequestURL());
            logger.info("context path = " + request.getContextPath());
            logger.info("request content type = " + request.getContentType());
            logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        }

    }
}
