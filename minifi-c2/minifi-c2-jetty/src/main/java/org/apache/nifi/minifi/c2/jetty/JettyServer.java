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

import org.apache.nifi.minifi.c2.api.properties.C2Properties;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JettyServer {
    private static final Logger logger = LoggerFactory.getLogger(JettyServer.class);
    private static String C2_SERVER_HOME = System.getenv("C2_SERVER_HOME");
    private static final String WEB_DEFAULTS_XML = "webdefault.xml";

    public static void main(String[] args) throws Exception {
        C2Properties properties = C2Properties.getInstance();

        final HandlerCollection handlers = new HandlerCollection();
        for (Path path : Files.list(Paths.get(C2_SERVER_HOME, "webapps")).collect(Collectors.toList())) {
             handlers.addHandler(loadWar(path.toFile(), "/c2", JettyServer.class.getClassLoader()));
        }

        Server server;
        int port = Integer.parseInt(properties.getProperty("minifi.c2.server.port", "10080"));
        SslContextFactory sslContextFactory = properties.getSslContextFactory();
        if (sslContextFactory == null) {
            server = new Server(port);
        } else {
            HttpConfiguration config = new HttpConfiguration();
            config.setSecureScheme("https");
            config.setSecurePort(port);
            config.addCustomizer(new SecureRequestCustomizer());

            server = new Server();

            ServerConnector serverConnector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, "http/1.1"), new HttpConnectionFactory(config));
            serverConnector.setPort(port);

            server.addConnector(serverConnector);
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

    private static WebAppContext loadWar(final File warFile, final String contextPath, final ClassLoader parentClassLoader) throws IOException {
        final WebAppContext webappContext = new WebAppContext(warFile.getPath(), contextPath);
        webappContext.setContextPath(contextPath);
        webappContext.setDisplayName(contextPath);

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

        webappContext.setClassLoader(new WebAppClassLoader(parentClassLoader, webappContext));

        logger.info("Loading WAR: " + warFile.getAbsolutePath() + " with context path set to " + contextPath);
        return webappContext;
    }
}
