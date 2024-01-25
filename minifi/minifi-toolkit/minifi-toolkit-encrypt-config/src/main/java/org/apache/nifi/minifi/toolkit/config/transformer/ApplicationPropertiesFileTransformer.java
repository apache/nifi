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
package org.apache.nifi.minifi.toolkit.config.transformer;

import static org.apache.nifi.properties.ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.properties.ApplicationProperties;
import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.properties.ProtectedPropertyContext;
import org.apache.nifi.properties.SensitivePropertyProvider;

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
        ApplicationProperties applicationProperties,
        SensitivePropertyProvider outputSensitivePropertyProvider,
        Set<String> sensitivePropertyNames
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
    public void transform(Path inputPath, Path outputPath) throws IOException {
        Objects.requireNonNull(inputPath, "Input path required");
        Objects.requireNonNull(outputPath, "Output path required");

        try (BufferedReader reader = Files.newBufferedReader(inputPath);
            BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            transform(reader, writer);
        }
    }

    private void transform(BufferedReader reader, BufferedWriter writer) throws IOException {
        String line = reader.readLine();
        while (line != null) {
            Matcher matcher = PROPERTY_VALUE_PATTERN.matcher(line);
            String nextLine = null;
            if (matcher.matches()) {
                nextLine = processPropertyLine(reader, writer, matcher, line);
            } else {
                writeNonSensitiveLine(writer, line);
            }

            line = nextLine == null ? reader.readLine() : nextLine;
        }
    }

    private String processPropertyLine(BufferedReader reader, BufferedWriter writer, Matcher matcher, String line) throws IOException {
        String actualPropertyName = matcher.group(NAME_GROUP);
        String actualPropertyValue = matcher.group(VALUE_GROUP);
        String nextLine = null;

        if (!actualPropertyName.endsWith(PROTECTED_KEY_SUFFIX)) {
            nextLine = reader.readLine();
            if (sensitivePropertyNames.contains(actualPropertyName) || isNextPropertyProtected(actualPropertyName, nextLine)) {
                String applicationProperty = applicationProperties.getProperty(actualPropertyName, actualPropertyValue);
                writeProtectedProperty(writer, actualPropertyName, applicationProperty);
            } else {
                writeNonSensitiveLine(writer, line);
            }
        }
        return nextLine;
    }

    private void writeNonSensitiveLine(BufferedWriter writer, String line) throws IOException {
        writer.write(line);
        writer.newLine();
    }

    private boolean isNextPropertyProtected(String actualPropertyName, String nextLine) {
        if (nextLine != null) {
            Matcher nextLineMatcher = PROPERTY_VALUE_PATTERN.matcher(nextLine);
            if (nextLineMatcher.matches()) {
                String protectedActualPropertyName = ApplicationPropertiesProtector.getProtectionKey(actualPropertyName);
                String nextName = nextLineMatcher.group(NAME_GROUP);
                return protectedActualPropertyName.equals(nextName);
            }
        }
        return false;
    }

    private void writeProtectedProperty(BufferedWriter writer, String name, String value) throws IOException {
        ProtectedPropertyContext propertyContext = ProtectedPropertyContext.defaultContext(name);
        String protectedValue = outputSensitivePropertyProvider.protect(value, propertyContext);

        writer.write(name);
        writer.write(PROPERTY_VALUE_SEPARATOR);
        writeNonSensitiveLine(writer, protectedValue);

        String protectedName = ApplicationPropertiesProtector.getProtectionKey(name);
        writer.write(protectedName);
        writer.write(PROPERTY_VALUE_SEPARATOR);
        String protectionIdentifierKey = outputSensitivePropertyProvider.getIdentifierKey();
        writeNonSensitiveLine(writer, protectionIdentifierKey);
    }
}
