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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.Request;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.c2.client.api.ConfigurationFileHolder;
import org.apache.nifi.c2.client.api.Differentiator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class WholeConfigDifferentiatorTest {

    public static final Path newConfigPath = Paths.get("./src/test/resources/config.yml");
    public static final Path defaultConfigPath = Paths.get("./src/test/resources/default.yml");

    public static ByteBuffer defaultConfigBuffer;
    public static ByteBuffer newConfigBuffer;
    public static Properties properties = new Properties();
    public static ConfigurationFileHolder configurationFileHolder;

    public static Request dummyRequest;

    @BeforeAll
    public static void setConfiguration() throws IOException {
        dummyRequest = new Request.Builder()
                .get()
                .url("https://nifi.apache.org/index.html")
                .build();

        defaultConfigBuffer = ByteBuffer.wrap(FileUtils.readFileToByteArray(defaultConfigPath.toFile()));
        newConfigBuffer = ByteBuffer.wrap(FileUtils.readFileToByteArray(newConfigPath.toFile()));

        configurationFileHolder = Mockito.mock(ConfigurationFileHolder.class);

        when(configurationFileHolder.getConfigFileReference()).thenReturn(new AtomicReference<>(defaultConfigBuffer));
    }

    // InputStream differentiator methods

    @Test
    public void TestSameInputStream() throws IOException {
        Differentiator<InputStream> differentiator = WholeConfigDifferentiator.getInputStreamDifferentiator();
        differentiator.initialize(properties, configurationFileHolder);

        FileInputStream fileInputStream = new FileInputStream(defaultConfigPath.toFile());
        assertFalse(differentiator.isNew(fileInputStream));
    }

    @Test
    public void TestNewInputStream() throws IOException {
        Differentiator<InputStream> differentiator = WholeConfigDifferentiator.getInputStreamDifferentiator();
        differentiator.initialize(properties, configurationFileHolder);

        FileInputStream fileInputStream = new FileInputStream(newConfigPath.toFile());
        assertTrue(differentiator.isNew(fileInputStream));
    }

    // Bytebuffer differentiator methods

    @Test
    public void TestSameByteBuffer() throws IOException {
        Differentiator<ByteBuffer> differentiator = WholeConfigDifferentiator.getByteBufferDifferentiator();
        differentiator.initialize(properties, configurationFileHolder);

        assertFalse(differentiator.isNew(defaultConfigBuffer));
    }

    @Test
    public void TestNewByteBuffer() throws IOException {
        Differentiator<ByteBuffer> differentiator = WholeConfigDifferentiator.getByteBufferDifferentiator();
        differentiator.initialize(properties, configurationFileHolder);

        assertTrue(differentiator.isNew(newConfigBuffer));
    }
}
