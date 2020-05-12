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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.mockito.Mockito.verify;

public class ConfigurationChangeCoordinatorTest {

    private ConfigurationChangeCoordinator coordinatorSpy;
    private Properties properties = new Properties();

    @Before
    public void setUp() throws Exception {
        coordinatorSpy = Mockito.spy(new ConfigurationChangeCoordinator());
    }

    @After
    public void tearDown() throws Exception {
        coordinatorSpy.close();
    }

    @Test
    public void testInit() throws Exception {
        properties.put("nifi.minifi.notifier.ingestors", "org.apache.nifi.minifi.bootstrap.configuration.ingestors.RestChangeIngestor");
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Collections.singleton(testListener));
    }

    @Test
    public void testNotifyListeners() throws Exception {
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Collections.singleton(testListener));

        Assert.assertEquals("Did not receive the correct number of registered listeners", coordinatorSpy.getChangeListeners().size(), 1);

        coordinatorSpy.notifyListeners(ByteBuffer.allocate(1));

        verify(testListener, Mockito.atMost(1)).handleChange(Mockito.any(InputStream.class));
    }

    @Test
    public void testRegisterListener() throws Exception {
        final ConfigurationChangeListener firstListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Collections.singleton(firstListener));

        Assert.assertEquals("Did not receive the correct number of registered listeners", coordinatorSpy.getChangeListeners().size(), 1);

        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Arrays.asList(firstListener, firstListener));
        Assert.assertEquals("Did not receive the correct number of registered listeners", coordinatorSpy.getChangeListeners().size(), 1);

        final ConfigurationChangeListener secondListener = Mockito.mock(ConfigurationChangeListener.class);
        coordinatorSpy.initialize(properties, Mockito.mock(ConfigurationFileHolder.class), Arrays.asList(firstListener, secondListener));
        Assert.assertEquals("Did not receive the correct number of registered listeners", coordinatorSpy.getChangeListeners().size(), 2);

    }
}
