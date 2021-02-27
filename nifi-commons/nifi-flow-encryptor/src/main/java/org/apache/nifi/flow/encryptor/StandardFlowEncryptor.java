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
package org.apache.nifi.flow.encryptor;

import org.apache.nifi.encrypt.PropertyEncryptor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard Flow Encryptor handles reading Input Steam and writing Output Stream
 */
public class StandardFlowEncryptor implements FlowEncryptor {
    private static final Pattern ENCRYPTED_PATTERN = Pattern.compile("enc\\{([^\\}]+?)\\}");

    private static final int FIRST_GROUP = 1;

    private static final String ENCRYPTED_FORMAT = "enc{%s}";

    /**
     * Process Flow Configuration Stream replacing existing encrypted properties with new encrypted properties
     *
     * @param inputStream Flow Configuration Input Stream
     * @param outputStream Flow Configuration Output Stream encrypted using new password
     * @param inputEncryptor Property Encryptor for Input Configuration
     * @param outputEncryptor Property Encryptor for Output Configuration
     */
    @Override
    public void processFlow(final InputStream inputStream, final OutputStream outputStream, final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) {
        try (final PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream))) {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                reader.lines().forEach(line -> {
                    final Matcher matcher = ENCRYPTED_PATTERN.matcher(line);
                    if (matcher.find()) {
                        final String outputEncrypted = getOutputEncrypted(matcher.group(FIRST_GROUP), inputEncryptor, outputEncryptor);
                        final String outputLine = matcher.replaceFirst(outputEncrypted);
                        writer.println(outputLine);
                    } else {
                        writer.println(line);
                    }
                });
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed Processing Flow Configuration", e);
        }
    }

    private String getOutputEncrypted(final String inputEncrypted, final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) {
        final String inputDecrypted = inputEncryptor.decrypt(inputEncrypted);
        final String outputEncrypted = outputEncryptor.encrypt(inputDecrypted);
        return String.format(ENCRYPTED_FORMAT, outputEncrypted);
    }
}
