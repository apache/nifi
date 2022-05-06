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

package org.apache.nifi.minifi.bootstrap.configuration;

import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

public class ConfigurationChangeCoordinatorTest {

    private ConfigurationChangeCoordinator coordinatorSpy;
    private final Properties properties = new Properties();

    @BeforeEach
    public void setUp() {
        coordinatorSpy = Mockito.spy(new ConfigurationChangeCoordinator());
    }

    @AfterEach
    public void tearDown() throws Exception {
        coordinatorSpy.close();
    }

    @Test
    public void testInit() {
        properties.put("nifi.minifi.notifier.ingestors", "org.apache.nifi.minifi.bootstrap.configuration.ingestors.RestChangeIngestor");
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Collections.singleton(testListener));
    }

    @Test
    public void testNotifyListeners() throws Exception {
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Collections.singleton(testListener));

        assertEquals(coordinatorSpy.getChangeListeners().size(), 1, "Did not receive the correct number of registered listeners");

        coordinatorSpy.notifyListeners(ByteBuffer.allocate(1));

        verify(testListener, Mockito.atMost(1)).handleChange(Mockito.any(InputStream.class));
    }

    @Test
    public void testRegisterListener() {
        final ConfigurationChangeListener firstListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Collections.singleton(firstListener));

        assertEquals(coordinatorSpy.getChangeListeners().size(), 1, "Did not receive the correct number of registered listeners");

        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Arrays.asList(firstListener, firstListener));
        assertEquals(coordinatorSpy.getChangeListeners().size(), 1, "Did not receive the correct number of registered listeners");

        final ConfigurationChangeListener secondListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Arrays.asList(firstListener, secondListener));
        assertEquals(coordinatorSpy.getChangeListeners().size(), 2, "Did not receive the correct number of registered listeners");

    }
}
