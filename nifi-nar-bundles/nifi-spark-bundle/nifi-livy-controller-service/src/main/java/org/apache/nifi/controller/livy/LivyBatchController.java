package org.apache.nifi.controller.livy;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.api.livy.LivyBatch;
import org.apache.nifi.controller.api.livy.LivyBatchService;
import org.apache.nifi.controller.api.livy.LivySessionService;
import org.apache.nifi.controller.api.livy.exception.SessionManagerException;
import org.apache.nifi.controller.livy.utilities.LivyHelpers;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.ssl.SSLContextService;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"Livy", "REST", "Spark", "http"})
@CapabilityDescription("Submits Spark batch jobs over HTTP")
public class LivyBatchController extends AbstractControllerService implements LivyBatchService {

    private volatile String livyUrl;
    private volatile String proxyUser;
    private volatile String queue;
    private volatile String driverMemory;
    private volatile Integer driverCores;
    private volatile String executorMemory;
    private volatile Integer executorCores;
    private volatile Integer numExecutors;
    private volatile String conf;

    private volatile String jars;
    private volatile String files;
    private volatile String pyFiles;
    private volatile String archives;
    private volatile SSLContextService sslContextService;
    private volatile int connectTimeout;
    private volatile boolean enabled = true;
    private volatile KerberosCredentialsService credentialsService;
    private volatile String credentialPrincipal;

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(ControllerServiceInitializationContext config) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(LivyHelpers.LIVY_HOST);
        props.add(LivyHelpers.LIVY_PORT);
        props.add(LivyHelpers.QUEUE);
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

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        final String livyHost = context.getProperty(LivyHelpers.LIVY_HOST).evaluateAttributeExpressions().getValue();
        final String livyPort = context.getProperty(LivyHelpers.LIVY_PORT).evaluateAttributeExpressions().getValue();
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
        final String conf = context.getProperty(LivyHelpers.CONF).evaluateAttributeExpressions().getValue();

        sslContextService = context.getProperty(LivyHelpers.SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        connectTimeout = Math.toIntExact(context.getProperty(LivyHelpers.CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));
        credentialsService = context.getProperty(LivyHelpers.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        this.livyUrl = "http" + (sslContextService != null ? "s" : "") + "://" + livyHost + ":" + livyPort;
        this.jars = jars;
        this.files = files;
        this.pyFiles = pyFiles;
        this.archives = archives;
        this.proxyUser = proxyUser;
        this.queue = queue;
        this.driverMemory = driverMemory;
        this.driverCores = driverCores;
        this.executorMemory = executorMemory;
        this.executorCores = executorCores;
        this.numExecutors = numExecutors;
        this.conf = conf;
        this.enabled = true;

        // Store a copy of the credentialsService principal name for easy string matching
        if(credentialsService != null) {
            credentialPrincipal = credentialsService.getPrincipal();
            credentialPrincipal = credentialPrincipal.substring(0, credentialPrincipal.indexOf("@"));
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public LivyBatch getBatchSession(Integer batchid) throws SessionManagerException {
        String sessionsUrl = livyUrl + "/batches/" + batchid;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", APPLICATION_JSON);
        headers.put("X-Requested-By", USER);
        try {
            JSONObject sessionsInfo = LivyHelpers.readJSONFromUrl(
                    sslContextService, credentialsService, connectTimeout,sessionsUrl, headers);

            LivyBatch livyBatch = new LivyBatch();
            livyBatch.id = batchid;
            livyBatch.appId = sessionsInfo.getString("appId");
            livyBatch.state = sessionsInfo.getString("state");

            // Get log list, which is a list of String
            JSONArray logList = sessionsInfo.getJSONArray("log");
            if(logList != null){
                livyBatch.log = new String[logList.length()];
                for(int l=0;l<logList.length();l++){
                    livyBatch.log[l] = logList.getString(l);
                }
            }

            return livyBatch;
        } catch (JSONException|IOException e) {
            throw new SessionManagerException(e);
        }
    }

    @Override
    public String getBatchStatus(Integer batchid) throws SessionManagerException {
        // GET /batches/{batchId}/state
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", LivySessionService.APPLICATION_JSON);
        headers.put("X-Requested-By", LivySessionService.USER);
        headers.put("Accept", "application/json");

        try {
            JSONObject batchState = LivyHelpers.readJSONFromUrl(sslContextService,
                    credentialsService, connectTimeout, livyUrl + "/batches/" + batchid + "/state",
                    headers);

            return batchState.getString("state");
        } catch(JSONException|IOException e){
            throw new SessionManagerException(e);
        }
    }

    @Override
    public Integer submitBatch(String codePath, String className,
                               String codeArgs) throws SessionManagerException {

        ComponentLog log = getLogger();
        String batchUrl = livyUrl + "/batches";
        JSONObject output = null;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", LivySessionService.APPLICATION_JSON);
        headers.put("X-Requested-By", LivySessionService.USER);

        log.debug("submitBatch() Submitting Batch Job to Spark via: " + batchUrl);

        StringBuilder payload = new StringBuilder("{\"file\":\"" + codePath + "\"");
        appendValue(payload, "className", className);

        try {
            if (codeArgs != null) {
                payload.append(",\"args\":");
                payload.append(splitCollect(codeArgs));
            }
            if (jars != null) {
                payload.append(",\"jars\":");
                payload.append(splitCollect(jars));
            }
            if (files != null) {
                payload.append(",\"files\":");
                payload.append(splitCollect(files));
            }
            if (pyFiles != null) {
                payload.append(",\"pyFiles\":");
                payload.append(splitCollect(pyFiles));
            }
            if (archives != null) {
                payload.append(",\"archives\":");
                payload.append(splitCollect(archives));
            }
        } catch (IOException e){
            throw new SessionManagerException(e);
        }

        appendValue(payload, "proxyUser", proxyUser);
        appendValue(payload, "queue", queue);
        appendValue(payload, "driverMemory", driverMemory);
        appendValue(payload, "driverCores", driverCores);
        appendValue(payload, "executorMemory", executorMemory);
        appendValue(payload, "executorCores", executorCores);
        appendValue(payload, "numExecutors", numExecutors);
        appendSubObject(payload, "conf", conf);

        payload.append("}");

        log.debug("submitBatch() Payload: " + payload.toString());

        try {
            output = LivyHelpers.readJSONObjectFromUrlPOST(
                    sslContextService, credentialsService, connectTimeout, batchUrl, headers, payload.toString());

            return output.getInt("id");
        } catch (JSONException|IOException e){
            throw new SessionManagerException(e);
        }
    }

    final ObjectMapper mapper = new ObjectMapper();

    private void appendSubObject(StringBuilder payload, String key, String val){
        if (val != null) {
            payload.append(",\"" + key + "\": ");
            payload.append(val);
        }
    }

    private void appendValue(StringBuilder payload, String key, String val){
        if (val != null) {
            payload.append(",\"" + key + "\": \"");
            payload.append(val);
            payload.append("\"");
        }
    }

    private void appendValue(StringBuilder payload, String key, Integer val){
        if (val != null) {
            payload.append(",\"" + key + "\": ");
            payload.append(val);
        }
    }

    private String splitCollect(String arg) throws IOException {
        List<String> filesArray = Arrays.stream(arg.split(","))
                .filter(StringUtils::isNotBlank)
                .map(String::trim).collect(Collectors.toList());
        String filesJsonArray = mapper.writeValueAsString(filesArray);

        return filesJsonArray;
    }
}
