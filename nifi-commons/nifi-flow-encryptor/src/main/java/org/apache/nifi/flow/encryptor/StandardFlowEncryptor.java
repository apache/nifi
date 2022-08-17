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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * Standard Flow Encryptor handles reading Input Steam and writing Output Stream
 */
public class StandardFlowEncryptor implements FlowEncryptor {
    private static final int XML_DECLARATION = '<';

    /**
     * Process Flow Configuration Stream replacing existing encrypted properties with new encrypted properties
     *
     * @param inputStream Flow Configuration Input Stream
     * @param outputStream Flow Configuration Output Stream encrypted using new password
     * @param inputEncryptor Property Encryptor for Input Configuration
     * @param outputEncryptor Property Encryptor for Output Configuration
     */
    @Override
    public void processFlow(final InputStream inputStream, final OutputStream outputStream,
                            final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) {
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        bufferedInputStream.mark(1);
        try {
            final int firstByte = bufferedInputStream.read();
            bufferedInputStream.reset();
            final FlowEncryptor flowEncryptor = (firstByte == XML_DECLARATION) ? new XmlFlowEncryptor() : new JsonFlowEncryptor();
            flowEncryptor.processFlow(bufferedInputStream, outputStream, inputEncryptor, outputEncryptor);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
