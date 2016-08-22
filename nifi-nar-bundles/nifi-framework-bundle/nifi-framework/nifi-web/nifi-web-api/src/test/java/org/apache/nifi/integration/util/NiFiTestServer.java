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
package org.apache.nifi.integration.util;

import com.sun.jersey.api.client.Client;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.util.WebUtils;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContext;
import java.io.File;
import java.util.Collections;

/**
 * Creates an embedded server for testing the NiFi REST API.
 */
public class NiFiTestServer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiTestServer.class);

    private Server jetty;
    private final NiFiProperties properties;
    private WebAppContext webappContext;

    public NiFiTestServer(String webappRoot, String contextPath, NiFiProperties properties) {
        this.properties = properties;

        createWebAppContext(webappRoot, contextPath);
        createServer();
    }

    private WebAppContext createWebAppContext(String webappRoot, String contextPath) {
        webappContext = new WebAppContext();
        webappContext.setContextPath(contextPath);
        webappContext.setWar(webappRoot);
        webappContext.setLogUrlOnStart(true);
        webappContext.setTempDirectory(new File("target/jetty"));
        return webappContext;
    }

    private Server createServer() {
        jetty = new Server(0);

        createSecureConnector();

        jetty.setHandler(webappContext);
        return jetty;
    }

    private void createSecureConnector() {
        org.eclipse.jetty.util.ssl.SslContextFactory contextFactory = new org.eclipse.jetty.util.ssl.SslContextFactory();

        // require client auth when not supporting login or anonymous access
        if (StringUtils.isBlank(properties.getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER))) {
            contextFactory.setNeedClientAuth(true);
        } else {
            contextFactory.setWantClientAuth(true);
        }

        /* below code sets JSSE system properties when values are provided */
        // keystore properties
        if (StringUtils.isNotBlank(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE))) {
            contextFactory.setKeyStorePath(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE));
        }
        if (StringUtils.isNotBlank(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE))) {
            contextFactory.setKeyStoreType(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));
        }
        final String keystorePassword = properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD);
        final String keyPassword = properties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD);
        if (StringUtils.isNotBlank(keystorePassword)) {
            // if no key password was provided, then assume the keystore password is the same as the key password.
            final String defaultKeyPassword = (StringUtils.isBlank(keyPassword)) ? keystorePassword : keyPassword;
            contextFactory.setKeyManagerPassword(keystorePassword);
            contextFactory.setKeyStorePassword(defaultKeyPassword);
        } else if (StringUtils.isNotBlank(keyPassword)) {
            // since no keystore password was provided, there will be no keystore integrity check
            contextFactory.setKeyStorePassword(keyPassword);
        }

        // truststore properties
        if (StringUtils.isNotBlank(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE))) {
            contextFactory.setTrustStorePath(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));
        }
        if (StringUtils.isNotBlank(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE))) {
            contextFactory.setTrustStoreType(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
        }
        if (StringUtils.isNotBlank(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD))) {
            contextFactory.setTrustStorePassword(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));
        }

        // add some secure config
        final HttpConfiguration httpsConfiguration = new HttpConfiguration();
        httpsConfiguration.setSecureScheme("https");
        httpsConfiguration.setSecurePort(0);
        httpsConfiguration.addCustomizer(new SecureRequestCustomizer());

        // build the connector
        final ServerConnector https = new ServerConnector(jetty,
                new SslConnectionFactory(contextFactory, "http/1.1"),
                new HttpConnectionFactory(httpsConfiguration));

        // set host and port
        https.setPort(0);

        // add the connector
        jetty.addConnector(https);
    }

    public void startServer() throws Exception {
        jetty.start();

        // ensure the ui extensions are set
        webappContext.getServletContext().setAttribute("nifi-ui-extensions", new UiExtensionMapping(Collections.EMPTY_MAP));
    }

    public void loadFlow() throws Exception {
        logger.info("Loading Flow...");

        // start and load the flow
        FlowService flowService = getSpringBean("flowService", FlowService.class);
        flowService.load(null);

        logger.info("Flow loaded successfully.");
    }

    public void shutdownServer() throws Exception {
        jetty.stop();
    }

    public int getPort() {
        if (!jetty.isStarted()) {
            throw new IllegalStateException("Jetty server not started");
        }
        return ((ServerConnector) jetty.getConnectors()[1]).getLocalPort();
    }

    public String getBaseUrl() {
        return "https://localhost:" + getPort();
    }

    public Client getClient() {
        return WebUtils.createClient(null, SslContextFactory.createSslContext(properties));
    }

    /**
     * Convenience method to provide access to Spring beans accessible from the
     * web application context.
     *
     * @param <T> target cast
     * @param beanName name of the spring bean
     * @param clazz class of the spring bean
     * @return Spring bean with given name and class type
     *
     * @throws ClassCastException if the bean found cannot be cast to the given
     * class type
     */
    public <T> T getSpringBean(String beanName, Class<T> clazz) {
        ServletContext servletContext = webappContext.getServletHandler().getServletContext();
        WebApplicationContext webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);
        return clazz.cast(webApplicationContext.getBean(beanName));
    }
}
