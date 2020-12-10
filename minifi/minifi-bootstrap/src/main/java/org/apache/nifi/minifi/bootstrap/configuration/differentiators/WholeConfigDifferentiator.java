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

package org.apache.nifi.minifi.bootstrap.configuration.differentiators;

import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.interfaces.Differentiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public abstract class WholeConfigDifferentiator {


    private final static Logger logger = LoggerFactory.getLogger(WholeConfigDifferentiator.class);

    public static final String WHOLE_CONFIG_KEY = "Whole Config";

    volatile ConfigurationFileHolder configurationFileHolder;

    boolean compareInputStreamToConfigFile(InputStream inputStream) throws IOException {
        logger.debug("Checking if change is different");
        AtomicReference<ByteBuffer> currentConfigFileReference = configurationFileHolder.getConfigFileReference();
        ByteBuffer currentConfigFile = currentConfigFileReference.get();
        ByteBuffer byteBuffer = ByteBuffer.allocate(currentConfigFile.limit());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        try {
            dataInputStream.readFully(byteBuffer.array());
        } catch (EOFException e) {
            logger.debug("New config is shorter than the current. Must be different.");
            return true;
        }
        logger.debug("Read the input");

        if (dataInputStream.available() != 0) {
            return true;
        } else {
            return byteBuffer.compareTo(currentConfigFile) != 0;
        }
    }

    public void initialize(Properties properties, ConfigurationFileHolder configurationFileHolder) {
        this.configurationFileHolder = configurationFileHolder;
    }


    public static class InputStreamInput extends WholeConfigDifferentiator implements Differentiator<InputStream> {
        public boolean isNew(InputStream inputStream) throws IOException {
            return compareInputStreamToConfigFile(inputStream);
        }
    }

    public static class ByteBufferInput extends WholeConfigDifferentiator implements Differentiator<ByteBuffer> {
        public boolean isNew(ByteBuffer inputBuffer) {
            AtomicReference<ByteBuffer> currentConfigFileReference = configurationFileHolder.getConfigFileReference();
            ByteBuffer currentConfigFile = currentConfigFileReference.get();
            return inputBuffer.compareTo(currentConfigFile) != 0;
        }
    }


    public static Differentiator<InputStream> getInputStreamDifferentiator() {
        return new InputStreamInput();
    }

    public static Differentiator<ByteBuffer> getByteBufferDifferentiator() {
        return new ByteBufferInput();
    }
}
