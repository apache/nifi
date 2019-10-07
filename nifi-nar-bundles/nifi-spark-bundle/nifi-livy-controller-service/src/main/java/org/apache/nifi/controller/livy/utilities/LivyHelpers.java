package org.apache.nifi.controller.livy.utilities;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.KerberosKeytabCredentials;
import org.apache.nifi.hadoop.KerberosKeytabSPNegoAuthSchemeProvider;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;

public class LivyHelpers {
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

    public static final PropertyDescriptor SESSION_TYPE = new PropertyDescriptor.Builder()
            .name("livy-cs-session-kind")
            .displayName("Session Type")
            .description("The type of Spark session to start (spark, pyspark, pyspark3, sparkr, e.g.)")
            .required(true)
            .allowableValues("spark", "pyspark", "pyspark3", "sparkr")
            .defaultValue("spark")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_USER = new PropertyDescriptor.Builder()
            .name("livy-cs-session-proxyuser")
            .displayName("Proxy User")
            .description("User to impersonate when starting the session")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
            .name("livy-cs-session-queue")
            .displayName("YARN Queue")
            .description("The name of the YARN queue to which submitted")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DRIVER_MEMORY = new PropertyDescriptor.Builder()
            .name("livy-cs-session-driver-memory")
            .displayName("Driver Memory")
            .description("Amount of memory to use for the driver process")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DRIVER_CORES = new PropertyDescriptor.Builder()
            .name("livy-cs-session-driver-cores")
            .displayName("Driver Cores")
            .description("Number of cores to use for the driver process")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EXECUTOR_MEMORY = new PropertyDescriptor.Builder()
            .name("livy-cs-session-executor-memory")
            .displayName("Executor Memory")
            .description("Amount of memory to use per executor process")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EXECUTOR_CORES = new PropertyDescriptor.Builder()
            .name("livy-cs-session-executor-cores")
            .displayName("Executor Cores")
            .description("Number of cores to use for each executor")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EXECUTOR_COUNT = new PropertyDescriptor.Builder()
            .name("livy-cs-session-executor-count")
            .displayName("Executor Count")
            .description("Number of executors to launch for this session")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CONF = new PropertyDescriptor.Builder()
            .name("livy-cs-session-conf")
            .displayName("Conf")
            .description("Spark configuration properties, in JSON format. Ex: {\"spark.driver.maxResultSize\": \"0\", \"spark.submit.deployMode\": \"cluster\"}")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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

    public static final PropertyDescriptor PY_FILES = new PropertyDescriptor.Builder()
            .name("livy-cs-session-py-files")
            .displayName("Session Python Files")
            .description("Python files to be used in this session")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.FILE_EXISTS_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue(null)
            .build();

    public static final PropertyDescriptor ARCHIVES = new PropertyDescriptor.Builder()
            .name("livy-cs-session-archives")
            .displayName("Session Archives")
            .description("Archives to be used in this session")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.FILE_EXISTS_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue(null)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("exec-spark-iactive-charset")
            .displayName("Character Set")
            .description("The character set encoding for the incoming flow file.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static CloseableHttpClient openConnection(SSLContextService sslContextService,
                                            KerberosCredentialsService credentialsService,
                                            int connectTimeout) throws IOException {
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

    public static JSONObject readJSONObjectFromUrlPOST(
            SSLContextService sslContextService,
            KerberosCredentialsService credentialsService,
            int connectTimeout,
            String urlString, Map<String, String> headers, String payload) throws IOException, JSONException {

        try(CloseableHttpClient httpClient = openConnection(sslContextService, credentialsService, connectTimeout)){
            HttpPost request = new HttpPost(urlString);
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.addHeader(entry.getKey(), entry.getValue());
            }

            if(payload != null) {
                HttpEntity httpEntity = new StringEntity(payload);
                request.setEntity(httpEntity);
            }

            HttpResponse response = httpClient.execute(request);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode() + " : " + response.getStatusLine().getReasonPhrase());
            }

            InputStream content = response.getEntity().getContent();
            return readAllIntoJSONObject(content);
        }
    }

    public static JSONObject readJSONFromUrl(
            SSLContextService sslContextService,
            KerberosCredentialsService credentialsService,
            int connectTimeout,
            String urlString, Map<String, String> headers) throws IOException, JSONException {

        try(CloseableHttpClient httpClient = openConnection(sslContextService, credentialsService, connectTimeout)) {
            HttpGet request = new HttpGet(urlString);
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.addHeader(entry.getKey(), entry.getValue());
            }
            HttpResponse response = httpClient.execute(request);

            InputStream content = response.getEntity().getContent();
            return readAllIntoJSONObject(content);
        }
    }

    public static JSONObject readJSONFromUrlDELETE(
            SSLContextService sslContextService,
            KerberosCredentialsService credentialsService,
            int connectTimeout,
            String urlString, Map<String, String> headers) throws IOException, JSONException {

        try(CloseableHttpClient httpClient = openConnection(sslContextService, credentialsService, connectTimeout)) {
            HttpDelete request = new HttpDelete(urlString);
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.addHeader(entry.getKey(), entry.getValue());
            }
            HttpResponse response = httpClient.execute(request);

            InputStream content = response.getEntity().getContent();
            return readAllIntoJSONObject(content);
        }
    }

    public static JSONObject readAllIntoJSONObject(InputStream content) throws IOException, JSONException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8));
        String jsonText = IOUtils.toString(rd);
        return new JSONObject(jsonText);
    }

    public static SSLContext getSslSocketFactory(SSLContextService sslService)
            throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException {

        SSLContext sslContext = sslService.createSSLContext(SSLContextService.ClientAuth.NONE);
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
}
