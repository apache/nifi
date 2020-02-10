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
package org.apache.nifi.processor.util.http;

import com.burgstaller.okhttp.AuthenticationCacheInterceptor;
import com.burgstaller.okhttp.CachingAuthenticatorDecorator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.burgstaller.okhttp.digest.DigestAuthenticator;
import java.io.IOException;
import java.net.Proxy;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.ProxyAuthenticator;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.security.util.SSLConfig;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains utility methods which are commonly needed when constructing
 * {@code OkHttp} components from NiFi-specific configurations. As OkHttp is currently
 * the library of choice for HTTP interaction, these methods are made available to multiple
 * components here rather than duplicate a large amount of code throughout individual
 * extensions.
 */
public class OkHttpUtils {
    public static final String PROXY_HOST_NAME = "http-proxy-host";
    public static final String PROXY_PORT_NAME = "http-proxy-port";
    public static final String PROXY_USERNAME_NAME = "http-proxy-username";
    public static final String PROXY_PASSWORD_NAME = "http-proxy-password";
    public static final String CONNECTION_TIMEOUT_NAME = "http-connect-timeout";
    public static final String READ_TIMEOUT_NAME = "http-response-timeout";
    public static final String WRITE_TIMEOUT_NAME = "http-write-timeout";
    public static final String SSL_CS_NAME = "ssl-context-service";
    public static final Map<String, String> DEFAULT_PD_MAPPING = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(OkHttpUtils.class);

    public static OkHttpClient.Builder buildOkHttpClientFromProcessorConfig(ProcessContext context) {
        return buildOkHttpClientFromProcessorConfig(context, DEFAULT_PD_MAPPING);
    }

    public static OkHttpClient.Builder buildOkHttpClientFromProcessorConfig(PropertyContext context, Map<String, String> pdMap) {
        // Initialize the client builder
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

        // Start by checking for proxy configuration
        // Add a proxy if set
        final ProxyConfiguration proxyConfig = getProxyConfigurationFromContext(context, pdMap);
        setProxyConfiguration(okHttpClientBuilder, proxyConfig);

        // Set connection, read, and write timeouts if present
        setTimeouts(okHttpClientBuilder, context, pdMap);

        // Set TLS configuration (if provided)
        final PropertyValue sslCSPV = getPropertyValue(context, SSL_CS_NAME, pdMap);
        if (sslCSPV != null) {
            final SSLContextService sslService = sslCSPV.asControllerService(SSLContextService.class);
            configureBuilderFromSSLContextService(okHttpClientBuilder, sslService);
        }
        return okHttpClientBuilder;
    }

    /**
     * Returns a {@link ProxyConfiguration} from {@link PropertyDescriptor}s set on the component directly.
     *
     * @param context the {@link PropertyContext}
     * @param pdMap   the mapping of standard to custom property descriptor names
     * @return the proxy configuration
     */
    private static ProxyConfiguration getProxyConfigurationFromContext(PropertyContext context, Map<String, String> pdMap) {
        return ProxyConfiguration.getConfiguration(context, () -> {
            final PropertyValue host = getPropertyValue(context, PROXY_HOST_NAME, pdMap);
            final PropertyValue port = getPropertyValue(context, PROXY_PORT_NAME, pdMap);
            if (host != null && port != null) {
                final String proxyHost = host.getValue();
                final Integer proxyPort = port.asInteger();
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                final PropertyValue username = getPropertyValue(context, PROXY_USERNAME_NAME, pdMap);
                final PropertyValue password = getPropertyValue(context, PROXY_PASSWORD_NAME, pdMap);
                if (username != null) {
                    componentProxyConfig.setProxyUserName(username.getValue());
                }
                if (password != null) {
                    componentProxyConfig.setProxyUserPassword(password.getValue());
                }
                return componentProxyConfig;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });
    }

    /**
     * Sets the proxy configuration on the client builder. If the {@link ProxyConfiguration#getProxyType()} is
     * {@link ProxyConfiguration#DIRECT_CONFIGURATION}, no action is taken.
     *
     * @param okHttpClientBuilder the client builder
     * @param proxyConfiguration  the proxy configuration
     */
    public static void setProxyConfiguration(OkHttpClient.Builder okHttpClientBuilder, ProxyConfiguration proxyConfiguration) {
        if (!Proxy.Type.DIRECT.equals(proxyConfiguration.getProxyType())) {
            if (proxyConfiguration.isValid()) {
                final Proxy proxy = proxyConfiguration.createProxy();
                okHttpClientBuilder.proxy(proxy);

                if (proxyConfiguration.hasCredential()) {
                    ProxyAuthenticator proxyAuthenticator = new ProxyAuthenticator(proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
                    okHttpClientBuilder.proxyAuthenticator(proxyAuthenticator);
                }
            }
        }
    }

    private static void setTimeouts(OkHttpClient.Builder okHttpClientBuilder, PropertyContext context, Map<String, String> pdMap) {
        // Set timeouts
        if (pdMap.containsKey(CONNECTION_TIMEOUT_NAME)) {
            final PropertyValue connectionTimeout = getPropertyValue(context, CONNECTION_TIMEOUT_NAME, pdMap);
            if (connectionTimeout.isSet()) {
                int connectionTimeoutMillis = connectionTimeout.asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                okHttpClientBuilder.connectTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        }

        if (pdMap.containsKey(READ_TIMEOUT_NAME)) {
            final PropertyValue readTimeout = getPropertyValue(context, READ_TIMEOUT_NAME, pdMap);
            if (readTimeout.isSet()) {
                int readTimeoutMillis = readTimeout.asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                okHttpClientBuilder.readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        }

        if (pdMap.containsKey(WRITE_TIMEOUT_NAME)) {
            final PropertyValue writeTimeout = getPropertyValue(context, WRITE_TIMEOUT_NAME, pdMap);
            if (writeTimeout.isSet()) {
                int writeTimeoutMillis = writeTimeout.asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                okHttpClientBuilder.writeTimeout(writeTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Returns the {@link PropertyValue} from the {@link PropertyContext} given the "standard" property name and the
     * {@link PropertyDescriptor} name mapping from the standard to the per-component property descriptor name. This
     * utility method is convenient because only {@link ProcessContext} allows for value retrieval by String name, while
     * the other implementations only support retrieval by {@link PropertyDescriptor}. Expression Language will be
     * evaluated if supported. If no property descriptor is set, returns {@code null}.
     * <p>
     * Example:
     * <p>
     * Context: {@code ["cxn-to": "1000 millis"]}
     * Processor connection timeout PD name: {@code "cxn-to"}
     * Standard connection timeout PD name: {@code CONNECTION_TIMEOUT_NAME = "http-connect-timeout"}
     * PD Map: {@code ["http-connect-timeout": "cxn-to"]}
     * <p>
     * Invocation:
     * <p>
     * {@code getPropertyValue(context, CONNECTION_TIMEOUT_NAME, pdMap) -> "1000 millis"}
     *
     * @param propertyContext the context (likely {@link ProcessContext} or {@link org.apache.nifi.controller.ConfigurationContext}
     * @param property        the "standard" property name (see {@link OkHttpUtils#DEFAULT_PD_MAPPING}
     * @param pdMap           the map of standard to customized property descriptor names
     * @return the parsed property value
     */
    private static PropertyValue getPropertyValue(PropertyContext propertyContext, String property, Map<String, String> pdMap) {
        // PropertyContext only allows access by PropertyDescriptor, not String, so build a temp PD; the equals comparison only uses name
        final String componentPropertyName = pdMap.get(property);
        if (StringUtils.isEmpty(componentPropertyName)) {
            return null;
        }
        PropertyDescriptor pd = new PropertyDescriptor.Builder().name(componentPropertyName).build();

        // If this is a ProcessContext, enumerate the property descriptors and retrieve the fully-populated PD which will indicate if EL is supported
        if (propertyContext instanceof ProcessContext) {
            ProcessContext processContext = (ProcessContext) propertyContext;
            List<PropertyDescriptor> pds = processContext.getProperties().keySet().stream()
                    .filter(p -> p.getName().equals(componentPropertyName)).collect(Collectors.toList());
            if (pds.size() > 0) {
                pd = pds.get(0);
            }
            if (pd.isExpressionLanguageSupported()) {
                return propertyContext.getProperty(pd).evaluateAttributeExpressions();
            } else {
                return propertyContext.getProperty(pd);
            }
        } else {
            // Trying to evaluate EL on a PD that doesn't support it will fail
            return propertyContext.getProperty(pd);
        }
    }

    /**
     * Configures the client builder with the TLS/SSL settings from the provided controller service.
     *
     * @param okHttpClientBuilder the client builder
     * @param sslService          the controller service with the provided configuration
     */
    public static void configureBuilderFromSSLContextService(OkHttpClient.Builder okHttpClientBuilder, SSLContextService sslService) {
        if (sslService == null) {
            logger.warn("No SSL Context Service available for OkHttp client builder");
            return;
        }
        try {
            // Get the SSL config from the controller service
            SSLConfig sslConfig = sslService.getSSLConfig();
            Tuple<SSLContext, TrustManager[]> sslContextTuple = SslContextFactory.createSslContextFromControllerService(sslConfig);
            List<X509TrustManager> x509TrustManagers = Arrays.stream(sslContextTuple.getValue())
                    .filter(trustManager -> trustManager instanceof X509TrustManager)
                    .map(trustManager -> (X509TrustManager) trustManager).collect(Collectors.toList());
            // Set the client factory to the one returned and the trust manager to the first in the list
            okHttpClientBuilder.sslSocketFactory(sslContextTuple.getKey().getSocketFactory(), x509TrustManagers.get(0));
        } catch (CertificateException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException | IOException e) {
            throw new ProcessException(e);
        }
    }

    /**
     * Adds digest authentication to the client builder.
     *
     * @param okHttpClientBuilder the client builder
     * @param username            the digest authentication username
     * @param password            the digest authentication password
     */
    public static void addDigestAuthenticator(OkHttpClient.Builder okHttpClientBuilder, String username, String password) {
        /*
         * OkHttp doesn't have built-in Digest Auth Support. A ticket for adding it is here[1] but they authors decided instead to rely on a 3rd party lib.
         *
         * [1] https://github.com/square/okhttp/issues/205#issuecomment-154047052
         */
        final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
        com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(username, password);
        final DigestAuthenticator digestAuthenticator = new DigestAuthenticator(credentials);

        okHttpClientBuilder.interceptors().add(new AuthenticationCacheInterceptor(authCache));
        okHttpClientBuilder.authenticator(new CachingAuthenticatorDecorator(digestAuthenticator, authCache));
    }
}
