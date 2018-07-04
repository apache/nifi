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
package org.apache.nifi.controller.livy;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.api.livy.exception.SessionManagerException;
import org.apache.nifi.hadoop.KerberosKeytabCredentials;
import org.apache.nifi.hadoop.KerberosKeytabSPNegoAuthSchemeProvider;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.nifi.controller.api.livy.LivySessionService;
import org.apache.nifi.expression.ExpressionLanguageScope;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

@Tags({"Livy", "REST", "Spark", "http"})
@CapabilityDescription("Manages pool of Spark sessions over HTTP")
public class LivySessionController extends AbstractControllerService implements LivySessionService {

    public static final PropertyDescriptor LIVY_HOST = new PropertyDescriptor.Builder()
            .name("livy-cs-livy-host")
            .displayName("Livy Host")
            .description("The hostname (or IP address) of the Livy server.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LIVY_PORT = new PropertyDescriptor.Builder()
            .name("livy-cs-livy-port")
            .displayName("Livy Port")
            .description("The port number for the Livy server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("8998")
            .build();

    public static final PropertyDescriptor SESSION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("livy-cs-session-pool-size")
            .displayName("Session Pool Size")
            .description("Number of sessions to keep open")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SESSION_TYPE = new PropertyDescriptor.Builder()
            .name("livy-cs-session-kind")
            .displayName("Session Type")
            .description("The type of Spark session to start (spark, pyspark, pyspark3, sparkr, e.g.)")
            .required(true)
            .allowableValues("spark", "pyspark", "pyspark3", "sparkr")
            .defaultValue("spark")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SESSION_MGR_STATUS_INTERVAL = new PropertyDescriptor.Builder()
            .name("livy-cs-session-manager-status-interval")
            .displayName("Session Manager Status Interval")
            .description("The amount of time to wait between requesting session information updates.")
            .required(true)
            .defaultValue("2 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JARS = new PropertyDescriptor.Builder()
            .name("livy-cs-session-jars")
            .displayName("Session JARs")
            .description("JARs to be used in the Spark session.")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.FILE_EXISTS_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor FILES = new PropertyDescriptor.Builder()
            .name("livy-cs-session-files")
            .displayName("Session Files")
            .description("Files to be used in the Spark session.")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.FILE_EXISTS_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue(null)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL (https) connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Max wait time for connection to remote service.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
        .name("kerberos-credentials-service")
        .displayName("Kerberos Credentials Service")
        .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
        .identifiesControllerService(KerberosCredentialsService.class)
        .required(false)
        .build();

    private volatile String livyUrl;
    private volatile int sessionPoolSize;
    private volatile String controllerKind;
    private volatile String jars;
    private volatile String files;
    private volatile Map<Integer, JSONObject> sessions = new ConcurrentHashMap<>();
    private volatile SSLContextService sslContextService;
    private volatile SSLContext sslContext;
    private volatile int connectTimeout;
    private volatile Thread livySessionManagerThread = null;
    private volatile boolean enabled = true;
    private volatile KerberosCredentialsService credentialsService;
    private volatile SessionManagerException sessionManagerException;

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(ControllerServiceInitializationContext config) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(LIVY_HOST);
        props.add(LIVY_PORT);
        props.add(SESSION_POOL_SIZE);
        props.add(SESSION_TYPE);
        props.add(SESSION_MGR_STATUS_INTERVAL);
        props.add(SSL_CONTEXT_SERVICE);
        props.add(CONNECT_TIMEOUT);
        props.add(JARS);
        props.add(FILES);
        props.add(KERBEROS_CREDENTIALS_SERVICE);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        final String livyHost = context.getProperty(LIVY_HOST).evaluateAttributeExpressions().getValue();
        final String livyPort = context.getProperty(LIVY_PORT).evaluateAttributeExpressions().getValue();
        final String sessionPoolSize = context.getProperty(SESSION_POOL_SIZE).evaluateAttributeExpressions().getValue();
        final String sessionKind = context.getProperty(SESSION_TYPE).getValue();
        final long sessionManagerStatusInterval = context.getProperty(SESSION_MGR_STATUS_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final String jars = context.getProperty(JARS).evaluateAttributeExpressions().getValue();
        final String files = context.getProperty(FILES).evaluateAttributeExpressions().getValue();
        sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        sslContext = sslContextService == null ? null : sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);
        connectTimeout = Math.toIntExact(context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
        credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        this.livyUrl = "http" + (sslContextService != null ? "s" : "") + "://" + livyHost + ":" + livyPort;
        this.controllerKind = sessionKind;
        this.jars = jars;
        this.files = files;
        this.sessionPoolSize = Integer.valueOf(sessionPoolSize);
        this.enabled = true;

        livySessionManagerThread = new Thread(() -> {
            while (enabled) {
                try {
                    manageSessions();
                    sessionManagerException = null;
                } catch (Exception e) {
                    getLogger().error("Livy Session Manager Thread run into an error, but continues to run", e);
                    sessionManagerException = new SessionManagerException(e);
                }
                try {
                    Thread.sleep(sessionManagerStatusInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    enabled = false;
                }
            }
        });
        livySessionManagerThread.setName("Livy-Session-Manager-" + controllerKind);
        livySessionManagerThread.start();
    }

    @OnDisabled
    public void shutdown() {
        ComponentLog log = getLogger();
        try {
            enabled = false;
            livySessionManagerThread.interrupt();
            livySessionManagerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Livy Session Manager Thread interrupted");
        }
    }

    @Override
    public Map<String, String> getSession() throws SessionManagerException {
        checkSessionManagerException();

        Map<String, String> sessionMap = new HashMap<>();
        try {
            final Map<Integer, JSONObject> sessionsCopy = sessions;
            for (int sessionId : sessionsCopy.keySet()) {
                JSONObject currentSession = sessions.get(sessionId);
                String state = currentSession.getString("state");
                String sessionKind = currentSession.getString("kind");
                if (state.equalsIgnoreCase("idle") && sessionKind.equalsIgnoreCase(controllerKind)) {
                    sessionMap.put("sessionId", String.valueOf(sessionId));
                    sessionMap.put("livyUrl", livyUrl);
                    break;
                }
            }
        } catch (JSONException e) {
            getLogger().error("Unexpected data found when looking for JSON object with 'state' and 'kind' fields", e);
        }
        return sessionMap;
    }

    @Override
    public HttpClient getConnection() throws IOException, SessionManagerException {
        checkSessionManagerException();

        return openConnection();
    }

    private HttpClient openConnection() throws IOException {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

        if (sslContextService != null) {
            try {
                SSLContext sslContext = getSslSocketFactory(sslContextService);
                httpClientBuilder.setSSLContext(sslContext);
            } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyManagementException e) {
                throw new IOException(e);
            }
        }

        if (credentialsService != null) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(new AuthScope(null, -1, null),
                new KerberosKeytabCredentials(credentialsService.getPrincipal(), credentialsService.getKeytab()));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider> create()
                .register(AuthSchemes.SPNEGO, new KerberosKeytabSPNegoAuthSchemeProvider()).build();
            httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
        }

        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        requestConfigBuilder.setConnectTimeout(connectTimeout);
        requestConfigBuilder.setConnectionRequestTimeout(connectTimeout);
        requestConfigBuilder.setSocketTimeout(connectTimeout);
        httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());

        return httpClientBuilder.build();
    }

    private void manageSessions() throws InterruptedException, IOException {
        int idleSessions = 0;
        JSONObject newSessionInfo;
        Map<Integer, JSONObject> sessionsInfo;
        ComponentLog log = getLogger();

        try {
            sessionsInfo = listSessions();
            if (sessions.isEmpty()) {
                log.debug("manageSessions() the active session list is empty, populating from acquired list...");
                sessions.putAll(sessionsInfo);
            }
            for (Integer sessionId : new ArrayList<>(sessions.keySet())) {
                JSONObject currentSession = sessions.get(sessionId);
                log.debug("manageSessions() Updating current session: " + currentSession);
                if (sessionsInfo.containsKey(sessionId)) {
                    String state = currentSession.getString("state");
                    String sessionKind = currentSession.getString("kind");
                    log.debug("manageSessions() controller kind: {}, session kind: {}, session state: {}",
                            new Object[]{controllerKind, sessionKind, state});
                    if (state.equalsIgnoreCase("idle") && sessionKind.equalsIgnoreCase(controllerKind)) {
                        // Keep track of how many sessions are in an idle state and thus available
                        idleSessions++;
                        sessions.put(sessionId, sessionsInfo.get(sessionId));
                        // Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                    } else if ((state.equalsIgnoreCase("busy") || state.equalsIgnoreCase("starting")) && sessionKind.equalsIgnoreCase(controllerKind)) {
                        // Update status of existing sessions
                        sessions.put(sessionId, sessionsInfo.get(sessionId));
                        // Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                    } else {
                        // Prune sessions of kind != controllerKind and whose state is:
                        // not_started, shutting_down, error, dead, success (successfully stopped)
                        sessions.remove(sessionId);
                        //Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                    }
                } else {
                    // Prune sessions that no longer exist
                    log.debug("manageSessions() session exists in session pool but not in source snapshot, removing from pool...");
                    sessions.remove(sessionId);
                    // Remove session from session list source of truth snapshot since it has been dealt with
                    sessionsInfo.remove(sessionId);
                }
            }
            int numSessions = sessions.size();
            log.debug("manageSessions() There are " + numSessions + " sessions in the pool");
            // Open new sessions equal to the number requested by sessionPoolSize
            if (numSessions == 0) {
                for (int i = 0; i < sessionPoolSize; i++) {
                    newSessionInfo = openSession();
                    sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
                    log.debug("manageSessions() Registered new session: " + newSessionInfo);
                }
            } else {
                // Open one new session if there are no idle sessions
                if (idleSessions == 0) {
                    log.debug("manageSessions() There are " + numSessions + " sessions in the pool but none of them are idle sessions, creating...");
                    newSessionInfo = openSession();
                    sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
                    log.debug("manageSessions() Registered new session: " + newSessionInfo);
                }
                // Open more sessions if number of sessions is less than target pool size
                if (numSessions < sessionPoolSize) {
                    log.debug("manageSessions() There are " + numSessions + ", need more sessions to equal requested pool size of " + sessionPoolSize + ", creating...");
                    for (int i = 0; i < sessionPoolSize - numSessions; i++) {
                        newSessionInfo = openSession();
                        sessions.put(newSessionInfo.getInt("id"), newSessionInfo);
                        log.debug("manageSessions() Registered new session: " + newSessionInfo);
                    }
                }
            }
        } catch (ConnectException | SocketTimeoutException ce) {
            log.error("Timeout connecting to Livy service to retrieve sessions", ce);
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    private Map<Integer, JSONObject> listSessions() throws IOException {
        String sessionsUrl = livyUrl + "/sessions";
        int numSessions;
        JSONObject sessionsInfo;
        Map<Integer, JSONObject> sessionsMap = new HashMap<>();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);
        try {
            sessionsInfo = readJSONFromUrl(sessionsUrl, headers);
            numSessions = sessionsInfo.getJSONArray("sessions").length();
            for (int i = 0; i < numSessions; i++) {
                int currentSessionId = sessionsInfo.getJSONArray("sessions").getJSONObject(i).getInt("id");
                JSONObject currentSession = sessionsInfo.getJSONArray("sessions").getJSONObject(i);
                sessionsMap.put(currentSessionId, currentSession);
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }

        return sessionsMap;
    }

    private JSONObject getSessionInfo(int sessionId) throws IOException {
        String sessionUrl = livyUrl + "/sessions/" + sessionId;
        JSONObject sessionInfo;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);
        try {
            sessionInfo = readJSONFromUrl(sessionUrl, headers);
        } catch (JSONException e) {
            throw new IOException(e);
        }

        return sessionInfo;
    }

    private JSONObject openSession() throws IOException, JSONException, InterruptedException {
        ComponentLog log = getLogger();
        JSONObject newSessionInfo;
        final ObjectMapper mapper = new ObjectMapper();

        String sessionsUrl = livyUrl + "/sessions";
        StringBuilder payload = new StringBuilder("{\"kind\":\"" + controllerKind + "\"");
        if (jars != null) {
            List<String> jarsArray = Arrays.stream(jars.split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim).collect(Collectors.toList());

            String jarsJsonArray = mapper.writeValueAsString(jarsArray);
            payload.append(",\"jars\":");
            payload.append(jarsJsonArray);
        }
        if (files != null) {
            List<String> filesArray = Arrays.stream(files.split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim).collect(Collectors.toList());
            String filesJsonArray = mapper.writeValueAsString(filesArray);
            payload.append(",\"files\":");
            payload.append(filesJsonArray);
        }

        payload.append("}");
        log.debug("openSession() Session Payload: " + payload.toString());
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);

        newSessionInfo = readJSONObjectFromUrlPOST(sessionsUrl, headers, payload.toString());
        Thread.sleep(1000);
        while (newSessionInfo.getString("state").equalsIgnoreCase("starting")) {
            log.debug("openSession() Waiting for session to start...");
            newSessionInfo = getSessionInfo(newSessionInfo.getInt("id"));
            log.debug("openSession() newSessionInfo: " + newSessionInfo);
            Thread.sleep(1000);
        }

        return newSessionInfo;
    }

    private JSONObject readJSONObjectFromUrlPOST(String urlString, Map<String, String> headers, String payload) throws IOException, JSONException {
        HttpClient httpClient = openConnection();

        HttpPost request = new HttpPost(urlString);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
        HttpEntity httpEntity = new StringEntity(payload);
        request.setEntity(httpEntity);
        HttpResponse response = httpClient.execute(request);

        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
            throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode() + " : " + response.getStatusLine().getReasonPhrase());
        }

        InputStream content = response.getEntity().getContent();
        return readAllIntoJSONObject(content);
    }

    private JSONObject readJSONFromUrl(String urlString, Map<String, String> headers) throws IOException, JSONException {
        HttpClient httpClient = openConnection();

        HttpGet request = new HttpGet(urlString);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
        HttpResponse response = httpClient.execute(request);

        InputStream content = response.getEntity().getContent();
        return readAllIntoJSONObject(content);
    }

    private JSONObject readAllIntoJSONObject(InputStream content) throws IOException, JSONException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8));
        String jsonText = IOUtils.toString(rd);
        return new JSONObject(jsonText);
    }

    private SSLContext getSslSocketFactory(SSLContextService sslService)
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {
        final String keystoreLocation = sslService.getKeyStoreFile();
        final String keystorePass = sslService.getKeyStorePassword();
        final String keystoreType = sslService.getKeyStoreType();

        // prepare the keystore
        final KeyStore keyStore = KeyStore.getInstance(keystoreType);

        try (FileInputStream keyStoreStream = new FileInputStream(keystoreLocation)) {
            keyStore.load(keyStoreStream, keystorePass.toCharArray());
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keystorePass.toCharArray());

        // load truststore
        final String truststoreLocation = sslService.getTrustStoreFile();
        final String truststorePass = sslService.getTrustStorePassword();
        final String truststoreType = sslService.getTrustStoreType();

        KeyStore truststore = KeyStore.getInstance(truststoreType);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        truststore.load(new FileInputStream(truststoreLocation), truststorePass.toCharArray());
        trustManagerFactory.init(truststore);

        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        return sslContext;
    }

    private void checkSessionManagerException() throws SessionManagerException {
        SessionManagerException exception = sessionManagerException;
        if (exception != null) {
            throw sessionManagerException;
        }
    }

}
