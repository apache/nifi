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

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.encryptor.FlowEncryptor;
import org.apache.nifi.flow.encryptor.JsonFlowEncryptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * File Transformer supporting transformation of Flow Configuration with sensitive property values
 */
public class FlowConfigurationFileTransformer implements FileTransformer {

    private final PropertyEncryptor inputEncryptor;

    private final PropertyEncryptor outputEncryptor;

    /**
     * Flow Configuration File Transformer with components required for decrypting input values and encrypting output values
     *
     * @param inputEncryptor Property Encryptor for decrypting input values
     * @param outputEncryptor Property Encryptor for encrypting output values
     */
    public FlowConfigurationFileTransformer(final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) {
        this.inputEncryptor = Objects.requireNonNull(inputEncryptor, "Input Encryptor required");
        this.outputEncryptor = Objects.requireNonNull(outputEncryptor, "Output Encryptor required");
    }

    @Override
    public void transform(final Path inputPath, final Path outputPath) throws IOException {
        Objects.requireNonNull(inputPath, "Input path required");
        Objects.requireNonNull(outputPath, "Output path required");

        try (
                InputStream inputStream = new GZIPInputStream(Files.newInputStream(inputPath));
                OutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(outputPath))
        ) {
            final FlowEncryptor flowEncryptor = new JsonFlowEncryptor();
            flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);
        }
    }
}
