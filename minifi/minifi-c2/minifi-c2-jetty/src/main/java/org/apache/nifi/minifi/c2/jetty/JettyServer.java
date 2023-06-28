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

package org.apache.nifi.minifi.c2.jetty;

import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEYSTORE;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEYSTORE_PASSWD;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEYSTORE_TYPE;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_KEY_PASSWD;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_TRUSTSTORE;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_TRUSTSTORE_PASSWD;
import static org.apache.nifi.minifi.c2.api.properties.C2Properties.MINIFI_C2_SERVER_TRUSTSTORE_TYPE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import org.apache.nifi.jetty.configuration.connector.StandardServerConnectorFactory;
import org.apache.nifi.minifi.c2.api.properties.C2Properties;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyServer {
    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private static final String C2_SERVER_HOME = System.getenv("C2_SERVER_HOME");
    private static final String WEB_DEFAULTS_XML = "webdefault.xml";

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public static void main(String[] args) throws Exception {
        C2Properties properties = C2Properties.getInstance();

        final HandlerCollection handlers = new HandlerCollection();
        try (Stream<Path> files = Files.list(Paths.get(C2_SERVER_HOME, "webapps"))) {
            files.forEach(path -> handlers.addHandler(loadWar(path.toFile(), "/c2", JettyServer.class.getClassLoader())));
        }

        Server server;
        int port = Integer.parseInt(properties.getProperty("minifi.c2.server.port", "10090"));
        if (properties.isSecure()) {
            server = new Server();
            StandardServerConnectorFactory serverConnectorFactory = new StandardServerConnectorFactory(server, port);
            serverConnectorFactory.setSslContext(buildSSLContext(properties));
            serverConnectorFactory.setWantClientAuth(true);

            ServerConnector https = serverConnectorFactory.getServerConnector();
            https.setPort(port);
            server.addConnector(https);
        } else {
            server = new Server(port);
        }

        server.setHandler(handlers);
        server.start();

        // ensure everything started successfully
        for (Handler handler : server.getChildHandlers()) {
            // see if the handler is a web app
            if (handler instanceof WebAppContext) {
                WebAppContext context = (WebAppContext) handler;

                // see if this webapp had any exceptions that would
                // cause it to be unavailable
                if (context.getUnavailableException() != null) {

                    System.err.println("Failed to start web server: " + context.getUnavailableException().getMessage());
                    System.err.println("Shutting down...");
                    logger.warn("Failed to start web server... shutting down.", context.getUnavailableException());
                    server.stop();
                    System.exit(1);
                }
            }
        }

        server.dumpStdErr();
        server.join();
    }

    private static SSLContext buildSSLContext(C2Properties properties) {
        KeyStore keyStore;
        KeyStore truststore;

        File keyStoreFile = Paths.get(C2_SERVER_HOME).resolve(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE)).toFile();
        logger.debug("Loading Key Store [{}]", keyStoreFile.getPath());
        try (FileInputStream keyStoreStream = new FileInputStream(keyStoreFile)) {
            keyStore = new StandardKeyStoreBuilder()
                .type(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE_TYPE))
                .inputStream(keyStoreStream)
                .password(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE_PASSWD).toCharArray())
                .build();
        } catch (IOException ioe) {
            throw new UncheckedIOException("Key Store loading failed", ioe);
        }

        File trustStoreFile = Paths.get(C2_SERVER_HOME).resolve(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE)).toFile();
        logger.debug("Loading Trust Store [{}]", trustStoreFile.getPath());
        try (FileInputStream trustStoreStream = new FileInputStream(trustStoreFile)) {
            truststore = new StandardKeyStoreBuilder()
                .type(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE_TYPE))
                .inputStream(trustStoreStream)
                .password(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE_PASSWD).toCharArray())
                .build();
        } catch (IOException ioe) {
            throw new UncheckedIOException("Trust Store loading failed", ioe);
        }

        return new StandardSslContextBuilder()
            .keyStore(keyStore)
            .keyPassword(properties.getProperty(MINIFI_C2_SERVER_KEY_PASSWD).toCharArray())
            .trustStore(truststore)
            .build();
    }

    private static WebAppContext loadWar(final File warFile, final String contextPath, final ClassLoader parentClassLoader) {
        final WebAppContext webappContext = new WebAppContext(warFile.getPath(), contextPath);
        webappContext.setContextPath(contextPath);
        webappContext.setDisplayName(contextPath);
        webappContext.setErrorHandler(getErrorHandler());

        // instruction jetty to examine these jars for tlds, web-fragments, etc
        webappContext.setAttribute("org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern", ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\\\.jar$|.*/[^/]*taglibs.*\\.jar$" );

        // remove slf4j server class to allow WAR files to have slf4j dependencies in WEB-INF/lib
        List<String> serverClasses = new ArrayList<>(Arrays.asList(webappContext.getServerClasses()));
        serverClasses.remove("org.slf4j.");
        webappContext.setServerClasses(serverClasses.toArray(new String[0]));
        webappContext.setDefaultsDescriptor(WEB_DEFAULTS_XML);

        // get the temp directory for this webapp
        File tempDir = Paths.get(C2_SERVER_HOME, "tmp", warFile.getName()).toFile();
        if (tempDir.exists() && !tempDir.isDirectory()) {
            throw new RuntimeException(tempDir.getAbsolutePath() + " is not a directory");
        } else if (!tempDir.exists()) {
            final boolean made = tempDir.mkdirs();
            if (!made) {
                throw new RuntimeException(tempDir.getAbsolutePath() + " could not be created");
            }
        }
        if (!(tempDir.canRead() && tempDir.canWrite())) {
            throw new RuntimeException(tempDir.getAbsolutePath() + " directory does not have read/write privilege");
        }

        // configure the temp dir
        webappContext.setTempDirectory(tempDir);

        // configure the max form size (3x the default)
        webappContext.setMaxFormContentSize(600000);

        try {
            webappContext.setClassLoader(new WebAppClassLoader(parentClassLoader, webappContext));
        } catch (IOException e) {
            throw new UncheckedIOException("ClassLoader initialization failed", e);
        }

        logger.info("Loading WAR: " + warFile.getAbsolutePath() + " with context path set to " + contextPath);
        return webappContext;
    }

    private static ErrorPageErrorHandler getErrorHandler() {
        final ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
        errorHandler.setShowServlet(false);
        errorHandler.setShowStacks(false);
        errorHandler.setShowMessageInTitle(false);
        return errorHandler;
    }
}
