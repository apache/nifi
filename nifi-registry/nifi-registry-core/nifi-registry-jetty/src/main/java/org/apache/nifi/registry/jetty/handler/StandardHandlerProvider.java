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
package org.apache.nifi.registry.jetty.handler;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.crypto.CryptoKeyProvider;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Standard Jetty Handler Provider responsible for loading web applications
 */
public class StandardHandlerProvider implements HandlerProvider {
    private static final String DEFAULTS_DESCRIPTOR = "org/apache/nifi-registry/web/webdefault.xml";

    private static final int MAX_FORM_CONTENT_SIZE = 600000;

    private static final String UI_CONTEXT_PATH = "/nifi-registry";

    private static final Pattern UI_FILE_PATTERN = Pattern.compile("^nifi-registry-web-ui-.+?\\.war$");

    private static final String API_CONTEXT_PATH = "/nifi-registry-api";

    private static final Pattern API_FILE_PATTERN = Pattern.compile("^nifi-registry-web-api-.+?\\.war$");

    private static final String DOCS_CONTEXT_PATH = "/nifi-registry-docs";

    private static final Pattern DOCS_FILE_PATTERN = Pattern.compile("^nifi-registry-web-docs-.+?\\.war$");

    private static final String HTML_DOCS_PATH = "/html/*";

    private static final String REST_API_DOCS_PATH = "/rest-api/*";

    private static final String REST_API_DOCS_RELATIVE_PATH = "webapp/docs";

    private static final String OIDC_SUPPORTED_PARAMETER = "oidc-supported";

    private static final String PROPERTIES_PARAMETER = "nifi-registry.properties";

    private static final String KEY_PROVIDER_PARAMETER = "nifi-registry.key";

    private static final String RESOURCE_BASE_PARAMETER = "resourceBase";

    private static final String DIR_ALLOWED_PARAMETER = "dirAllowed";

    private static final String WEB_INF_JAR_PATTERN_ATTRIBUTE = "org.eclipse.jetty.server.webapp.WebInfIncludeJarPattern";

    private static final String WEB_INF_JAR_PATTERN = ".*/spring-[^/]*\\.jar$";

    private final CryptoKeyProvider cryptoKeyProvider;

    private final String docsDirectory;

    public StandardHandlerProvider(final CryptoKeyProvider cryptoKeyProvider, final String docsDirectory) {
        this.cryptoKeyProvider = Objects.requireNonNull(cryptoKeyProvider, "Key Provider required");
        this.docsDirectory = docsDirectory;
    }

    /**
     * Get Jetty Handler for Registry Server containing mappings to web applications
     *
     * @param properties Registry properties
     * @return Jetty Handler
     */
    @Override
    public Handler getHandler(final NiFiRegistryProperties properties) {
        Objects.requireNonNull(properties, "Properties required");

        final File libDirectory = properties.getWarLibDirectory();
        final File workDirectory = properties.getWebWorkingDirectory();

        final HandlerCollection handlers = new HandlerCollection();
        // Add Header Writer Handler before others
        handlers.addHandler(new HeaderWriterHandler());

        final WebAppContext userInterfaceContext = getWebAppContext(libDirectory, workDirectory, ClassLoader.getSystemClassLoader(), UI_FILE_PATTERN, UI_CONTEXT_PATH);
        userInterfaceContext.setInitParameter(OIDC_SUPPORTED_PARAMETER, Boolean.toString(properties.isOidcEnabled()));
        handlers.addHandler(userInterfaceContext);

        final ClassLoader apiClassLoader = getApiClassLoader(properties.getDatabaseDriverDirectory());
        final WebAppContext apiContext = getWebAppContext(libDirectory, workDirectory, apiClassLoader, API_FILE_PATTERN, API_CONTEXT_PATH);
        apiContext.setAttribute(PROPERTIES_PARAMETER, properties);
        apiContext.setAttribute(KEY_PROVIDER_PARAMETER, cryptoKeyProvider);
        handlers.addHandler(apiContext);

        final WebAppContext docsContext = getWebAppContext(libDirectory, workDirectory, ClassLoader.getSystemClassLoader(), DOCS_FILE_PATTERN, DOCS_CONTEXT_PATH);
        final File docsDir = getDocsDir();
        final ServletHolder docsServletHolder = getDocsServletHolder(docsDir);
        docsContext.addServlet(docsServletHolder, HTML_DOCS_PATH);

        final File apiDocsDir = getApiDocsDir(apiContext);
        final ServletHolder apiDocsServletHolder = getDocsServletHolder(apiDocsDir);
        docsContext.addServlet(apiDocsServletHolder, REST_API_DOCS_PATH);

        handlers.addHandler(docsContext);

        return handlers;
    }

    private ClassLoader getApiClassLoader(final String databaseDriverDirectory) {
        final URL[] resourceLocations = getResourceLocations(databaseDriverDirectory);
        final ClassLoader apiClassLoader;
        if (resourceLocations.length == 0) {
            apiClassLoader = ClassLoader.getSystemClassLoader();
        } else {
            apiClassLoader = new URLClassLoader(resourceLocations, ClassLoader.getSystemClassLoader());
        }
        return apiClassLoader;
    }

    private WebAppContext getWebAppContext(
            final File libDirectory,
            final File workDirectory,
            final ClassLoader parentClassLoader,
            final Pattern applicationFilePattern,
            final String contextPath
    ) {
        final File applicationFile = getApplicationFile(libDirectory, applicationFilePattern);
        final WebAppContext webAppContext = new WebAppContext(applicationFile.getPath(), contextPath);
        webAppContext.setContextPath(contextPath);
        webAppContext.setDefaultsDescriptor(DEFAULTS_DESCRIPTOR);
        webAppContext.setMaxFormContentSize(MAX_FORM_CONTENT_SIZE);
        webAppContext.setAttribute(WEB_INF_JAR_PATTERN_ATTRIBUTE, WEB_INF_JAR_PATTERN);
        webAppContext.setErrorHandler(getErrorHandler());

        final File tempDirectory = getTempDirectory(workDirectory, applicationFile.getName());
        webAppContext.setTempDirectory(tempDirectory);

        try {
            final WebAppClassLoader webAppClassLoader = new WebAppClassLoader(parentClassLoader, webAppContext);
            webAppContext.setClassLoader(webAppClassLoader);
        } catch (final IOException e) {
            throw new IllegalStateException(String.format("Application Context Path [%s] ClassLoader creation failed", contextPath), e);
        }

        return webAppContext;
    }

    private File getApplicationFile(final File directory, final Pattern filenamePattern) {
        final File[] applicationFiles = directory.listFiles((file, filename) -> filenamePattern.matcher(filename).matches());
        if (applicationFiles == null || applicationFiles.length == 0) {
            throw new IllegalStateException(String.format("Required Application matching [%s] not found in directory [%s]", filenamePattern, directory));
        }
        return applicationFiles[0];
    }

    private ErrorPageErrorHandler getErrorHandler() {
        final ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
        errorHandler.setShowServlet(false);
        errorHandler.setShowStacks(false);
        errorHandler.setShowMessageInTitle(false);
        return errorHandler;
    }

    private File getTempDirectory(final File webWorkingDirectory, final String filename) {
        final File tempDirectory = new File(webWorkingDirectory, filename);
        if (tempDirectory.isDirectory()) {
            if (tempDirectory.canWrite()) {
                return tempDirectory;
            } else {
                throw new IllegalStateException(String.format("Temporary Directory [%s] not writable", tempDirectory));
            }
        } else {
            if (tempDirectory.mkdirs()) {
                return tempDirectory;
            } else {
                throw new IllegalStateException(String.format("Temporary Directory [%s] creation failed", tempDirectory));
            }
        }
    }

    private URL[] getResourceLocations(final String databaseDriverDirectory) {
        final URL[] resourceLocations;

        if (StringUtils.isBlank(databaseDriverDirectory)) {
            resourceLocations = new URL[0];
        } else {
            final File driverDirectory = new File(databaseDriverDirectory);
            if (driverDirectory.canRead()) {
                final List<URL> locations = new ArrayList<>();
                final URL driverDirectoryUrl = getUrl(driverDirectory);
                locations.add(driverDirectoryUrl);

                final File[] files = driverDirectory.listFiles();
                if (files != null) {
                    Arrays.stream(files)
                            .filter(File::isFile)
                            .map(this::getUrl)
                            .forEach(locations::add);
                }
                resourceLocations = locations.toArray(new URL[0]);
            } else {
                resourceLocations = new URL[0];
            }
        }
        return resourceLocations;
    }

    private URL getUrl(final File file) {
        try {
            return file.toURI().toURL();
        } catch (final MalformedURLException e) {
            throw new IllegalStateException(String.format("File URL [%s] conversion failed", file), e);
        }
    }

    private File getDocsDir() {
        File docsDir;
        try {
            docsDir = Paths.get(docsDirectory).toRealPath().toFile();
        } catch (IOException e) {
            docsDir = new File(docsDirectory).getAbsoluteFile();
            if (!docsDir.mkdirs()) {
                final String message = String.format("Documentation Directory [%s] creation failed", docsDir.getAbsolutePath());
                throw new IllegalStateException(message);
            }
        }
        return docsDir;
    }

    private ServletHolder getDocsServletHolder(final File directory) {
        final ServletHolder servletHolder = new ServletHolder(directory.getPath(), DefaultServlet.class);
        servletHolder.setInitParameter(RESOURCE_BASE_PARAMETER, directory.getPath());
        servletHolder.setInitParameter(DIR_ALLOWED_PARAMETER, Boolean.FALSE.toString());
        return servletHolder;
    }

    private File getApiDocsDir(final WebAppContext apiContext) {
        final File apiDocsDir = new File(apiContext.getTempDirectory(), REST_API_DOCS_RELATIVE_PATH);
        if (apiDocsDir.canRead() || apiDocsDir.mkdirs()) {
            return apiDocsDir;
        }
        throw new IllegalStateException(String.format("REST API Documentation Directory [%s] not readable", apiDocsDir.getAbsolutePath()));
    }
}
