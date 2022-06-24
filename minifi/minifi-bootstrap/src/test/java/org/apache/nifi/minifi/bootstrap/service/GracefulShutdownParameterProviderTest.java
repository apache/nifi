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

package org.apache.nifi.minifi.bootstrap.service;

import static org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider.DEFAULT_GRACEFUL_SHUTDOWN_VALUE;
import static org.apache.nifi.minifi.bootstrap.service.GracefulShutdownParameterProvider.GRACEFUL_SHUTDOWN_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GracefulShutdownParameterProviderTest {

    @Mock
    private BootstrapFileProvider bootstrapFileProvider;

    @InjectMocks
    private GracefulShutdownParameterProvider gracefulShutdownParameterProvider;

    @ParameterizedTest(name = "{index} => shutdownPropertyValue={0}")
    @NullSource
    @ValueSource(strings = {"notAnInteger", "-1"})
    void testGetBootstrapPropertiesShouldReturnDefaultShutdownPropertyValue(String shutdownProperty) throws IOException {
        Properties properties = new Properties();
        if (shutdownProperty != null) {
            properties.setProperty(GRACEFUL_SHUTDOWN_PROP, shutdownProperty);
        }
        when(bootstrapFileProvider.getBootstrapProperties()).thenReturn(properties);

        assertEquals(Integer.parseInt(DEFAULT_GRACEFUL_SHUTDOWN_VALUE), gracefulShutdownParameterProvider.getGracefulShutdownSeconds());
    }

    @Test
    void testGetBootstrapPropertiesShouldReturnShutdownPropertyValue() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(GRACEFUL_SHUTDOWN_PROP, "1000");
        when(bootstrapFileProvider.getBootstrapProperties()).thenReturn(properties);

        assertEquals(1000, gracefulShutdownParameterProvider.getGracefulShutdownSeconds());
    }

}