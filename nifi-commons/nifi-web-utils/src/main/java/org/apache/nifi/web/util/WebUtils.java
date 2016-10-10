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
package org.apache.nifi.web.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.CertificateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Common utilities related to web development.
 *
 */
public final class WebUtils {

    private static Logger logger = LoggerFactory.getLogger(WebUtils.class);

    final static ReadWriteLock lock = new ReentrantReadWriteLock();

    private WebUtils() {
    }

    /**
     * Creates a client for non-secure requests. The client will be created
     * using the given configuration. Additionally, the client will be
     * automatically configured for JSON serialization/deserialization.
     *
     * @param config client configuration
     *
     * @return a Client instance
     */
    public static Client createClient(final ClientConfig config) {
        return createClientHelper(config, null);
    }

    /**
     * Creates a client for secure requests. The client will be created using
     * the given configuration and security context. Additionally, the client
     * will be automatically configured for JSON serialization/deserialization.
     *
     * @param config client configuration
     * @param ctx security context
     *
     * @return a Client instance
     */
    public static Client createClient(final ClientConfig config, final SSLContext ctx) {
        return createClientHelper(config, ctx);
    }

    /**
     * A helper method for creating clients. The client will be created using
     * the given configuration and security context. Additionally, the client
     * will be automatically configured for JSON serialization/deserialization.
     *
     * @param config client configuration
     * @param ctx security context, which may be null for non-secure client
     * creation
     *
     * @return a Client instance
     */
    private static Client createClientHelper(final ClientConfig config, final SSLContext ctx) {

        final ClientConfig finalConfig = (config == null) ? new DefaultClientConfig() : config;

        if (ctx != null && StringUtils.isBlank((String) finalConfig.getProperty(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES))) {

            // custom hostname verifier that checks subject alternative names against the hostname of the URI
            final HostnameVerifier hostnameVerifier = new HostnameVerifier() {
                @Override
                public boolean verify(final String hostname, final SSLSession ssls) {

                    try {
                        for (final Certificate peerCertificate : ssls.getPeerCertificates()) {
                            if (peerCertificate instanceof X509Certificate) {
                                final X509Certificate x509Cert = (X509Certificate) peerCertificate;
                                final List<String> subjectAltNames = CertificateUtils.getSubjectAlternativeNames(x509Cert);
                                if (subjectAltNames.contains(hostname.toLowerCase())) {
                                    return true;
                                }
                            }
                        }
                    } catch (final SSLPeerUnverifiedException | CertificateParsingException ex) {
                        logger.warn("Hostname Verification encountered exception verifying hostname due to: " + ex, ex);
                    }

                    return false;
                }
            };

            finalConfig.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hostnameVerifier, ctx));
        }

        finalConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        finalConfig.getClasses().add(ObjectMapperResolver.class);

        // web client for restful request
        return Client.create(finalConfig);

    }

}
