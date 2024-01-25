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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File Transformer supporting Bootstrap Configuration with updated Root Key
 */
public class BootstrapConfigurationFileTransformer implements FileTransformer {

    private static final Pattern PROPERTY_VALUE_PATTERN = Pattern.compile("^([^#!][^=]+?)\\s*=.*");

    private static final int NAME_GROUP = 1;

    private static final char PROPERTY_VALUE_SEPARATOR = '=';

    private final String rootKeyPropertyName;

    private final String rootKey;

    /**
     * Bootstrap Configuration File Transformer writes provided Root Key to output files
     *
     * @param rootKeyPropertyName Root Key property name to be written
     * @param rootKey Root Key to be written
     */
    public BootstrapConfigurationFileTransformer(String rootKeyPropertyName, String rootKey) {
        this.rootKeyPropertyName = Objects.requireNonNull(rootKeyPropertyName, "Root Key Property Name required");
        this.rootKey = Objects.requireNonNull(rootKey, "Root Key required");
    }

    /**
     * Transform input configuration and write Root Key to output location
     *
     * @param inputPath Input file path to be transformed containing Bootstrap Configuration
     * @param outputPath Output file path for updated configuration
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
        boolean rootKeyPropertyNotFound = true;

        String line = reader.readLine();
        while (line != null) {
            Matcher matcher = PROPERTY_VALUE_PATTERN.matcher(line);
            if (matcher.matches()) {
                String name = matcher.group(NAME_GROUP);

                if (rootKeyPropertyName.equals(name)) {
                    writeRootKey(writer);
                    rootKeyPropertyNotFound = false;
                } else {
                    writer.write(line);
                    writer.newLine();
                }
            } else {
                writer.write(line);
                writer.newLine();
            }

            line = reader.readLine();
        }

        if (rootKeyPropertyNotFound) {
            writer.newLine();
            writeRootKey(writer);
        }
    }

    private void writeRootKey(BufferedWriter writer) throws IOException {
        writer.write(rootKeyPropertyName);
        writer.write(PROPERTY_VALUE_SEPARATOR);
        writer.write(rootKey);
        writer.newLine();
    }
}
