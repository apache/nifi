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
package org.apache.nifi.toolkit.config.transformer;

import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.properties.ProtectedPropertyContext;
import org.apache.nifi.properties.SensitivePropertyProvider;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File Transformer supporting transformation of Application Properties with sensitive property values
 */
public class ApplicationPropertiesFileTransformer implements FileTransformer {

    private static final Pattern PROPERTY_VALUE_PATTERN = Pattern.compile("^([^#!][^=]+?)\\s*=\\s?(.+)");

    private static final int NAME_GROUP = 1;

    private static final int VALUE_GROUP = 2;

    private static final char PROPERTY_VALUE_SEPARATOR = '=';

    private final ApplicationProperties applicationProperties;

    private final SensitivePropertyProvider outputSensitivePropertyProvider;

    private final Set<String> sensitivePropertyNames;

    /**
     * Application Properties File Transformer uses provided Application Properties as the source of protected values
     *
     * @param applicationProperties Application Properties containing decrypted source property values
     * @param outputSensitivePropertyProvider Sensitive Property Provider encrypts specified sensitive property values
     * @param sensitivePropertyNames Sensitive Property Names marked for encryption
     */
    public ApplicationPropertiesFileTransformer(
            final ApplicationProperties applicationProperties,
            final SensitivePropertyProvider outputSensitivePropertyProvider,
            final Set<String> sensitivePropertyNames
    ) {
        this.applicationProperties = Objects.requireNonNull(applicationProperties, "Application Properties required");
        this.outputSensitivePropertyProvider = Objects.requireNonNull(outputSensitivePropertyProvider, "Output Property Provider required");
        this.sensitivePropertyNames = Objects.requireNonNull(sensitivePropertyNames, "Sensitive Property Names required");
    }

    /**
     * Transform input application properties using configured Sensitive Property Provider and write output properties
     *
     * @param inputPath Input file path to be transformed containing source application properties
     * @param outputPath Output file path for protected application properties
     * @throws IOException Thrown on transformation failures
     */
    @Override
    public void transform(final Path inputPath, final Path outputPath) throws IOException {
        Objects.requireNonNull(inputPath, "Input path required");
        Objects.requireNonNull(outputPath, "Output path required");

        try (
                BufferedReader reader = Files.newBufferedReader(inputPath);
                BufferedWriter writer = Files.newBufferedWriter(outputPath)
        ) {
            transform(reader, writer);
        }
    }

    private void transform(final BufferedReader reader, final BufferedWriter writer) throws IOException {
        String line = reader.readLine();
        while (line != null) {
            final Matcher matcher = PROPERTY_VALUE_PATTERN.matcher(line);
            if (matcher.matches()) {
                final String name = matcher.group(NAME_GROUP);
                final String value = matcher.group(VALUE_GROUP);
                final String protectedName = ApplicationPropertiesProtector.getProtectionKey(name);

                final String nextLine = reader.readLine();
                if (nextLine == null) {
                    if (sensitivePropertyNames.contains(name)) {
                        writeProtectedProperty(writer, name, value);
                    } else {
                        writer.write(line);
                        writer.newLine();
                    }
                    break;
                }

                final Matcher nextLineMatcher = PROPERTY_VALUE_PATTERN.matcher(nextLine);
                if (nextLineMatcher.matches()) {
                    final String nextName = nextLineMatcher.group(NAME_GROUP);
                    final String nextValue = nextLineMatcher.group(VALUE_GROUP);

                    if (protectedName.equals(nextName)) {
                        // Read application property and write protected property
                        final String applicationProperty = applicationProperties.getProperty(name, value);
                        writeProtectedProperty(writer, name, applicationProperty);
                    } else {
                        if (sensitivePropertyNames.contains(name)) {
                            writeProtectedProperty(writer, name, value);
                        } else {
                            writer.write(line);
                            writer.newLine();
                        }
                        if (sensitivePropertyNames.contains(nextName)) {
                            writeProtectedProperty(writer, nextName, nextValue);
                        } else {
                            writer.write(nextLine);
                            writer.newLine();
                        }
                    }
                } else if (sensitivePropertyNames.contains(name)) {
                    writeProtectedProperty(writer, name, value);
                } else {
                    writer.write(line);
                    writer.newLine();
                    writer.write(nextLine);
                    writer.newLine();
                }
            } else {
                writer.write(line);
                writer.newLine();
            }

            line = reader.readLine();
        }
    }

    private void writeProtectedProperty(final BufferedWriter writer, final String name, final String value) throws IOException {
        final ProtectedPropertyContext propertyContext = ProtectedPropertyContext.defaultContext(name);
        final String protectedValue = outputSensitivePropertyProvider.protect(value, propertyContext);

        writer.write(name);
        writer.write(PROPERTY_VALUE_SEPARATOR);
        writer.write(protectedValue);
        writer.newLine();

        final String protectedName = ApplicationPropertiesProtector.getProtectionKey(name);
        writer.write(protectedName);
        writer.write(PROPERTY_VALUE_SEPARATOR);
        final String protectionIdentifierKey = outputSensitivePropertyProvider.getIdentifierKey();
        writer.write(protectionIdentifierKey);
        writer.newLine();
    }
}
