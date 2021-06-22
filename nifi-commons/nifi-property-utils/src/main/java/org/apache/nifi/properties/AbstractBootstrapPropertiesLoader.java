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
package org.apache.nifi.properties;

import org.apache.nifi.properties.BootstrapProperties.BootstrapPropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * An abstract base class for an application-specific BootstrapProperties loader.
 */
public abstract class AbstractBootstrapPropertiesLoader {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBootstrapPropertiesLoader.class);

    private static final String RELATIVE_APPLICATION_PROPERTIES_PATTERN = "conf/%s";
    private static final String BOOTSTRAP_CONF = "bootstrap.conf";

    /**
     * Return the property prefix used in the bootstrap.conf file for this application.
     * @return the property prefix
     */
    protected abstract String getApplicationPrefix();

    /**
     * Return the name of the main application properties file (e.g., nifi.properties).  This will be
     * used to determine the default location of the application properties file.
     * @return The name of the application properties file
     */
    protected abstract String getApplicationPropertiesFilename();

    /**
     * Return the system property name that should specify the file path of the main
     * application properties file.
     * @return The system property name that should provide the file path of the main application
     * properties file
     */
    protected abstract String getApplicationPropertiesFilePathSystemProperty();

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from
     * {@code $APPLICATION_HOME/conf/bootstrap.conf}.
     *
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    public String extractKeyFromBootstrapFile() throws IOException {
        return extractKeyFromBootstrapFile(null);
    }

    /**
     * Loads the bootstrap.conf file into a BootstrapProperties object.
     * @param bootstrapPath the path to the bootstrap file
     * @return The bootstrap.conf as a BootstrapProperties object
     * @throws IOException If the file is not readable
     */
    public BootstrapProperties loadBootstrapProperties(final String bootstrapPath) throws IOException {
        final Path bootstrapFilePath = getBootstrapFile(bootstrapPath).toPath();
       return loadBootstrapProperties(bootstrapFilePath, getApplicationPrefix());
    }

    /**
     * Loads a properties file into a BootstrapProperties object.
     * @param bootstrapPath The path to the properties file
     * @param propertyPrefix The property prefix to enforce
     * @return The BootstrapProperties
     * @throws IOException If the properties file could not be read
     */
    public static BootstrapProperties loadBootstrapProperties(final Path bootstrapPath, final String propertyPrefix) throws IOException {
        Objects.requireNonNull(bootstrapPath, "Bootstrap path must be provided");
        Objects.requireNonNull(propertyPrefix, "Property prefix must be provided");

        final Properties properties = new Properties();
        try (final InputStream bootstrapInput = Files.newInputStream(bootstrapPath)) {
            properties.load(bootstrapInput);
            return new BootstrapProperties(propertyPrefix, properties, bootstrapPath);
        } catch (final IOException e) {
            throw new IOException("Cannot read from " + bootstrapPath, e);
        }
    }

    /**
     * Returns the key (if any) used to encrypt sensitive properties, extracted from
     * {@code $APPLICATION_HOME/conf/bootstrap.conf}.
     *
     * @param bootstrapPath the path to the bootstrap file (if null, returns the sensitive key
     *                      found in $APPLICATION_HOME/conf/bootstrap.conf)
     * @return the key in hexadecimal format
     * @throws IOException if the file is not readable
     */
    public String extractKeyFromBootstrapFile(final String bootstrapPath) throws IOException {
        final BootstrapProperties bootstrapProperties = loadBootstrapProperties(bootstrapPath);

        return bootstrapProperties.getProperty(BootstrapPropertyKey.SENSITIVE_KEY).orElseGet(() -> {
            logger.warn("No encryption key present in the bootstrap.conf file at {}", bootstrapProperties.getConfigFilePath());
            return "";
        });
    }

    /**
     * Returns the file for bootstrap.conf.
     *
     * @param bootstrapPath the path to the bootstrap file (defaults to $APPLICATION_HOME/conf/bootstrap.conf
     *                     if null)
     * @return the {@code $APPLICATION_HOME/conf/bootstrap.conf} file
     * @throws IOException if the directory containing the file is not readable
     */
    private File getBootstrapFile(final String bootstrapPath) throws IOException {
        final File expectedBootstrapFile;
        if (bootstrapPath == null) {
            // Guess at location of bootstrap.conf file from nifi.properties file
            final String defaultApplicationPropertiesFilePath = getDefaultApplicationPropertiesFilePath();
            final File propertiesFile = new File(defaultApplicationPropertiesFilePath);
            final File confDir = new File(propertiesFile.getParent());
            if (confDir.exists() && confDir.canRead()) {
                expectedBootstrapFile = new File(confDir, BOOTSTRAP_CONF);
            } else {
                throw new IOException(String.format("Cannot read %s directory for %s", confDir, bootstrapPath));
            }
        } else {
            expectedBootstrapFile = new File(bootstrapPath);
        }

        if (expectedBootstrapFile.exists() && expectedBootstrapFile.canRead()) {
            return expectedBootstrapFile;
        } else {
            throw new IOException("Cannot read from " + expectedBootstrapFile.getAbsolutePath());
        }
    }

    /**
     * Returns the default file path to {@code $APPLICATION_HOME/conf/$APPLICATION.properties}. If the system
     * property provided by {@code AbstractBootstrapPropertiesLoader#getApplicationPropertiesFilePathSystemProperty()}
     * is not set, it will be set to the relative path provided by
     * {@code AbstractBootstrapPropertiesLoader#getRelativeApplicationPropertiesFilePath()}.
     *
     * @return the path to the application properties file
     */
    public String getDefaultApplicationPropertiesFilePath() {
        final String systemPropertyName = getApplicationPropertiesFilePathSystemProperty();
        final String defaultRelativePath = String.format(RELATIVE_APPLICATION_PROPERTIES_PATTERN, getApplicationPropertiesFilename());

        String systemPath = System.getProperty(systemPropertyName);

        if (systemPath == null || systemPath.trim().isEmpty()) {
            logger.warn("The system property {} is not set, so it is being set to '{}'", systemPropertyName, defaultRelativePath);
            System.setProperty(systemPropertyName, defaultRelativePath);
            systemPath = defaultRelativePath;
        }

        logger.info("Determined default application properties path to be '{}'", systemPath);
        return systemPath;
    }
}
