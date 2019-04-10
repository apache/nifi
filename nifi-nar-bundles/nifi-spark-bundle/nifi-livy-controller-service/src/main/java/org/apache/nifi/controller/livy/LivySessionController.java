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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.api.livy.exception.SessionManagerException;
import org.apache.nifi.controller.livy.utilities.LivyHelpers;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import org.apache.nifi.controller.api.livy.LivySessionService;
import org.apache.nifi.expression.ExpressionLanguageScope;

@Tags({"Livy", "REST", "Spark", "http"})
@CapabilityDescription("Manages pool of Spark sessions over HTTP")
public class LivySessionController extends AbstractControllerService implements LivySessionService {
    public static final PropertyDescriptor SESSION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("livy-cs-session-pool-size")
            .displayName("Min Session Pool Size")
            .description("Minimum number of sessions to keep open. If Elastic Session Pool is enabled, new sessions " +
                    "will be started automatically if no idle Livy sessions are available, up to the " +
                    "Max Session Pool Size.")
            .required(true)
            .defaultValue("2")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ELASTIC_SESSION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("livy-cs-elastic-session-pool-size")
            .displayName("Elastic Session Pool")
            .description("When enabled, if no 'idle' Livy sessions are available, a new session will be created, " +
                    "up to the Max Session Pool Size.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor MAX_SESSION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("livy-cs-max-session-pool-size")
            .displayName("Max Session Pool Size")
            .description("Maximum number of sessions to keep open. Only used when Elastic Session Pool is enabled." +
                    "A value of 0 means there is no limit on the number of Livy sessions.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

    public static final PropertyDescriptor SESSION_NAME = new PropertyDescriptor.Builder()
            .name("livy-cs-session-name")
            .displayName("Session Name")
            .description("Session Name is used to differentiate Livy Sessions, and ensure the controller service uses " +
                    "only sessions it is supposed to manage. If empty, defaults to the Controller Service UUID.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CLEANUP_SESSIONS = new PropertyDescriptor.Builder()
            .name("livy-cs-session-cleanup")
            .displayName("Cleanup Sessions")
            .description("When the Livy Controller Service shuts down, should created Livy sessions be deleted?")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor KEEP_SESSIONS_ALIVE = new PropertyDescriptor.Builder()
            .name("livy-cs-session-keep-alive")
            .displayName("Keep Busy Sessions Alive")
            .description("If a session is busy, should NiFi prevent Livy from terminating that session when the Livy " +
                    "idle timeout is reached?")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private volatile String livyUrl;
    private volatile int sessionPoolSize;
    private volatile boolean elasticSessionPool;
    private volatile int maxSessionPoolSize;
    private volatile String controllerKind;
    private volatile String proxyUser;
    private volatile String queue;
    private volatile String driverMemory;
    private volatile Integer driverCores;
    private volatile String executorMemory;
    private volatile Integer executorCores;
    private volatile Integer numExecutors;
    private volatile String sessionName;
    private volatile String conf;
    private volatile boolean cleanupSessions;
    private volatile boolean keepSessionsAlive;


    private volatile String jars;
    private volatile String files;
    private volatile String pyFiles;
    private volatile String archives;
    private volatile Map<Integer, JSONObject> sessions = new ConcurrentHashMap<>();
    private volatile SSLContextService sslContextService;
    private volatile int connectTimeout;
    private volatile Thread livySessionManagerThread = null;
    private volatile boolean enabled = true;
    private volatile KerberosCredentialsService credentialsService;
    private volatile String credentialPrincipal;
    private volatile String appropriateProxy;
    private volatile SessionManagerException sessionManagerException;

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(ControllerServiceInitializationContext config) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(LivyHelpers.LIVY_HOST);
        props.add(LivyHelpers.LIVY_PORT);
        props.add(SESSION_POOL_SIZE);
        props.add(ELASTIC_SESSION_POOL_SIZE);
        props.add(MAX_SESSION_POOL_SIZE);
        props.add(CLEANUP_SESSIONS);
        props.add(KEEP_SESSIONS_ALIVE);
        props.add(LivyHelpers.SESSION_TYPE);
        props.add(SESSION_NAME);
        props.add(LivyHelpers.QUEUE);
        props.add(SESSION_MGR_STATUS_INTERVAL);
        props.add(LivyHelpers.SSL_CONTEXT_SERVICE);
        props.add(LivyHelpers.CONNECT_TIMEOUT);
        props.add(LivyHelpers.JARS);
        props.add(LivyHelpers.FILES);
        props.add(LivyHelpers.PY_FILES);
        props.add(LivyHelpers.ARCHIVES);
        props.add(LivyHelpers.KERBEROS_CREDENTIALS_SERVICE);
        props.add(LivyHelpers.PROXY_USER);
        props.add(LivyHelpers.DRIVER_MEMORY);
        props.add(LivyHelpers.DRIVER_CORES);
        props.add(LivyHelpers.EXECUTOR_MEMORY);
        props.add(LivyHelpers.EXECUTOR_CORES);
        props.add(LivyHelpers.EXECUTOR_COUNT);
        props.add(LivyHelpers.CONF);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        final String livyHost = context.getProperty(LivyHelpers.LIVY_HOST).evaluateAttributeExpressions().getValue();
        final String livyPort = context.getProperty(LivyHelpers.LIVY_PORT).evaluateAttributeExpressions().getValue();
        final Integer sessionPoolSize = context.getProperty(SESSION_POOL_SIZE).evaluateAttributeExpressions().asInteger();
        final boolean elasticSessionPool = context.getProperty(ELASTIC_SESSION_POOL_SIZE).asBoolean();
        final Integer maxSessionPoolSize = context.getProperty(MAX_SESSION_POOL_SIZE).evaluateAttributeExpressions().asInteger();
        final String sessionKind = context.getProperty(LivyHelpers.SESSION_TYPE).getValue();
        final long sessionManagerStatusInterval = context.getProperty(SESSION_MGR_STATUS_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final String jars = context.getProperty(LivyHelpers.JARS).evaluateAttributeExpressions().getValue();
        final String files = context.getProperty(LivyHelpers.FILES).evaluateAttributeExpressions().getValue();
        final String pyFiles = context.getProperty(LivyHelpers.PY_FILES).evaluateAttributeExpressions().getValue();
        final String archives = context.getProperty(LivyHelpers.ARCHIVES).evaluateAttributeExpressions().getValue();

        final String proxyUser = context.getProperty(LivyHelpers.PROXY_USER).evaluateAttributeExpressions().getValue();
        final String queue = context.getProperty(LivyHelpers.QUEUE).evaluateAttributeExpressions().getValue();

        final String driverMemory = context.getProperty(LivyHelpers.DRIVER_MEMORY).evaluateAttributeExpressions().getValue();
        final Integer driverCores = context.getProperty(LivyHelpers.DRIVER_CORES).evaluateAttributeExpressions().asInteger();
        final String executorMemory = context.getProperty(LivyHelpers.EXECUTOR_MEMORY).evaluateAttributeExpressions().getValue();
        final Integer executorCores = context.getProperty(LivyHelpers.EXECUTOR_CORES).evaluateAttributeExpressions().asInteger();
        final Integer numExecutors = context.getProperty(LivyHelpers.EXECUTOR_COUNT).evaluateAttributeExpressions().asInteger();
        final String sessionName = context.getProperty(SESSION_NAME).evaluateAttributeExpressions().getValue();
        final String conf = context.getProperty(LivyHelpers.CONF).evaluateAttributeExpressions().getValue();
        final boolean cleanupSessions = context.getProperty(CLEANUP_SESSIONS).asBoolean();
        final boolean keepSessionsAlive = context.getProperty(KEEP_SESSIONS_ALIVE).asBoolean();

        sslContextService = context.getProperty(LivyHelpers.SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        connectTimeout = Math.toIntExact(context.getProperty(LivyHelpers.CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
        credentialsService = context.getProperty(LivyHelpers.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        this.livyUrl = "http" + (sslContextService != null ? "s" : "") + "://" + livyHost + ":" + livyPort;
        this.controllerKind = sessionKind;
        this.jars = jars;
        this.files = files;
        this.pyFiles = pyFiles;
        this.archives = archives;
        this.sessionPoolSize = sessionPoolSize;
        this.maxSessionPoolSize = maxSessionPoolSize;
        this.elasticSessionPool = elasticSessionPool;
        this.proxyUser = proxyUser;
        this.queue = queue;
        this.driverMemory = driverMemory;
        this.driverCores = driverCores;
        this.executorMemory = executorMemory;
        this.executorCores = executorCores;
        this.numExecutors = numExecutors;
        this.sessionName = StringUtils.isEmpty(sessionName)?this.getIdentifier():sessionName;
        this.conf = conf;
        this.cleanupSessions = cleanupSessions;
        this.keepSessionsAlive = keepSessionsAlive;
        this.enabled = true;

        // Store a copy of the credentialsService principal name for easy string matching
        if(credentialsService != null) {
            credentialPrincipal = credentialsService.getPrincipal();
            credentialPrincipal = credentialPrincipal.substring(0, credentialPrincipal.indexOf("@"));
            appropriateProxy = StringUtils.isEmpty(proxyUser)?credentialPrincipal:proxyUser;
        }

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

            // If we need to cleanup opened sessions
            if(this.cleanupSessions){
                for(Integer s : sessions.keySet()) {
                    // Don't re-throw session cleanup errors, just log
                    try {
                        JSONObject deleteMessage = this.deleteSession(s);
                    } catch (IOException e) {
                        log.warn("Livy Session Manager failed to cleanup session #" + s, e);
                    }
                }
            }
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

        return LivyHelpers.openConnection(sslContextService, credentialsService, connectTimeout);
    }

    private void manageSessions() throws InterruptedException, IOException {
        int idleSessions = 0;
        int startingSessions = 0;
        Map<Integer, JSONObject> idleSessionInfo = new HashMap<>();
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

                    String sessionOwner = currentSession.has("owner")?currentSession.getString("owner"):"";
                    String sessionProxy = currentSession.has("proxyUser")?currentSession.getString("proxyUser"):"";

                    log.debug("manageSessions() session ID: {}, session owner: {}, session proxy: {}, controller kind: {}, session kind: {}, session state: {}",
                            new Object[]{sessionId,sessionOwner, sessionProxy, controllerKind, sessionKind, state});

                    if (!sessionKind.equalsIgnoreCase(controllerKind)) {
                        // Prune sessions of kind != controllerKind, and where Owner is not contained in principal name
                        sessions.remove(sessionId);
                        //Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                        continue;
                    }

                    // If security is enabled, do owner/proxy checks
                    if (credentialsService != null &&
                            (!credentialPrincipal.equals(sessionOwner)
                                    || !appropriateProxy.equals(sessionProxy))) {

                        log.debug("manageSessions() Dropping session, not owned or proxied by this account. session ID: {}",
                                new Object[]{sessionId, sessionOwner, sessionProxy, controllerKind, sessionKind, state});
                        // where Owner or Proxy is not contained in principal name
                        sessions.remove(sessionId);
                        //Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                        continue;
                    }

                    // Status filters
                    if (state.equalsIgnoreCase("idle")) {
                        // Keep track of how many sessions are in an idle state and thus available
                        idleSessions++;
                        idleSessionInfo.put(sessionId, sessionsInfo.get(sessionId));
                        sessions.put(sessionId, sessionsInfo.get(sessionId));
                        // Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                    } else if ((state.equalsIgnoreCase("busy") || state.equalsIgnoreCase("starting"))) {
                        // Track starting instance count
                        if(state.equalsIgnoreCase("starting")){
                            startingSessions++;
                        }

                        // Update status of existing sessions
                        sessions.put(sessionId, sessionsInfo.get(sessionId));
                        // Remove session from session list source of truth snapshot since it has been dealt with
                        sessionsInfo.remove(sessionId);
                    } else {
                        // Prune sessions whose state is:
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
                // If we exceeded our session pool size, look for `idle` sessions we can shut down
                //  Two scenarios: we have no elastic pool sizing, in which case definitely look for candidates
                //  Or, we do have elastic pool sizing, in which case we need to make sure we are above our max
                //      pool size
                if(idleSessions > 0 && numSessions > sessionPoolSize && (!elasticSessionPool || numSessions > maxSessionPoolSize)) {
                    int sessionID = idleSessionInfo.keySet().stream().findFirst().get();
                    log.debug("manageSessions() There are " + numSessions + " sessions in the pool, " +
                            "this exceeds the maximum number of allowed sessions, shutting down idle Livy session " + sessionID + "...");
                    deleteSession(sessionID);
                }

                // If Elastic Session Pool sizing is enabled
                // Open one new session if there are no idle or starting sessions
                if (elasticSessionPool && numSessions < maxSessionPoolSize && (idleSessions + startingSessions) == 0) {
                    log.debug("manageSessions() There are " + numSessions + " sessions in the pool, " +
                            "none of them are idle sessions and Elastic Session Pool Sizing is enabled, creating...");
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

                // Sessions that are running may need to be "connected" to remotely to reset their timeouts.
                // Otherwise Livy will kill them based on bug LIVY-547.
                if(keepSessionsAlive){
                    for(Map.Entry<Integer, JSONObject> me : sessionsInfo.entrySet())
                    {
                        // Check if session is busy before updating it's activity date
                        String state = me.getValue().getString("state");
                        if(state.equalsIgnoreCase("busy")){
                            getSessionConnect(me.getKey());
                            log.debug("manageSessions() Connected to session to keep it alive: " + me.getKey());
                        }
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
            sessionsInfo = LivyHelpers.readJSONFromUrl(
                    sslContextService, credentialsService, connectTimeout,sessionsUrl, headers);
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
            sessionInfo = LivyHelpers.readJSONFromUrl(
                    sslContextService, credentialsService, connectTimeout, sessionUrl, headers);
        } catch (JSONException e) {
            throw new IOException(e);
        }

        return sessionInfo;
    }

    private JSONObject getSessionConnect(int sessionId) throws IOException {
        String sessionUrl = livyUrl + "/sessions/" + sessionId + "/connect";
        JSONObject sessionInfo;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);
        try {
            sessionInfo = LivyHelpers.readJSONObjectFromUrlPOST(
                    sslContextService, credentialsService, connectTimeout, sessionUrl, headers, null);
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
        if (pyFiles != null) {
            List<String> filesArray = Arrays.stream(pyFiles.split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim).collect(Collectors.toList());
            String filesJsonArray = mapper.writeValueAsString(filesArray);
            payload.append(",\"pyFiles\":");
            payload.append(filesJsonArray);
        }
        if (archives != null) {
            List<String> filesArray = Arrays.stream(archives.split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim).collect(Collectors.toList());
            String filesJsonArray = mapper.writeValueAsString(filesArray);
            payload.append(",\"archives\":");
            payload.append(filesJsonArray);
        }
        if (proxyUser != null) {
            payload.append(",\"proxyUser\": \"");
            payload.append(proxyUser);
            payload.append("\"");
        }
        if (queue != null) {
            payload.append(",\"queue\": \"");
            payload.append(queue);
            payload.append("\"");
        }
        if (driverMemory != null) {
            payload.append(",\"driverMemory\": \"");
            payload.append(driverMemory);
            payload.append("\"");
        }
        if (driverCores != null) {
            payload.append(",\"driverCores\": ");
            payload.append(driverCores);
        }
        if (executorMemory != null) {
            payload.append(",\"executorMemory\": \"");
            payload.append(executorMemory);
            payload.append("\"");
        }
        if (executorCores != null) {
            payload.append(",\"executorCores\": ");
            payload.append(executorCores);
        }
        if (numExecutors != null) {
            payload.append(",\"numExecutors\": ");
            payload.append(numExecutors);
        }
        if (sessionName != null) {
            payload.append(",\"name\": \"");
            payload.append(sessionName);
            payload.append("\"");
        }
        if (conf != null) {
            payload.append(",\"conf\": ");
            payload.append(conf);
        }

        payload.append("}");
        log.debug("openSession() Session Payload: " + payload.toString());
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);

        newSessionInfo = LivyHelpers.readJSONObjectFromUrlPOST(
                sslContextService, credentialsService, connectTimeout, sessionsUrl, headers, payload.toString());
        Thread.sleep(1000);
        while (newSessionInfo.getString("state").equalsIgnoreCase("starting")) {
            log.debug("openSession() Waiting for session to start...");
            newSessionInfo = getSessionInfo(newSessionInfo.getInt("id"));
            log.debug("openSession() newSessionInfo: " + newSessionInfo);
            Thread.sleep(1000);
        }

        return newSessionInfo;
    }

    private JSONObject deleteSession(int sessionId) throws IOException {
        String sessionUrl = livyUrl + "/sessions/" + sessionId;
        JSONObject sessionInfo;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);
        try {
            sessionInfo = LivyHelpers.readJSONFromUrlDELETE(
                    sslContextService, credentialsService, connectTimeout, sessionUrl, headers);
        } catch (JSONException e) {
            throw new IOException(e);
        }

        return sessionInfo;
    }

    private void checkSessionManagerException() throws SessionManagerException {
        SessionManagerException exception = sessionManagerException;
        if (exception != null) {
            throw sessionManagerException;
        }
    }

}
