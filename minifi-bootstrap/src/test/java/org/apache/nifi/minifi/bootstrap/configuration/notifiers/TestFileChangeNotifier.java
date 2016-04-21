/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi.bootstrap.configuration.notifiers;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.notifiers.FileChangeNotifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFileChangeNotifier {

    private static final String CONFIG_FILENAME = "config.yml";
    private static final String TEST_CONFIG_PATH = "src/test/resources/config.yml";

    private FileChangeNotifier notifierSpy;
    private WatchService mockWatchService;
    private Properties testProperties;

    @Before
    public void setUp() throws Exception {
        mockWatchService = Mockito.mock(WatchService.class);
        notifierSpy = Mockito.spy(new FileChangeNotifier());
        notifierSpy.setConfigFile(Paths.get(TEST_CONFIG_PATH));
        notifierSpy.setWatchService(mockWatchService);

        testProperties = new Properties();
        testProperties.put(FileChangeNotifier.CONFIG_FILE_PATH_KEY, TEST_CONFIG_PATH);
        testProperties.put(FileChangeNotifier.POLLING_PERIOD_INTERVAL_KEY, FileChangeNotifier.DEFAULT_POLLING_PERIOD_INTERVAL);
    }

    @After
    public void tearDown() throws Exception {
        notifierSpy.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testInitialize_invalidFile() throws Exception {
        testProperties.put(FileChangeNotifier.CONFIG_FILE_PATH_KEY, "/land/of/make/believe");
        notifierSpy.initialize(testProperties);
    }

    @Test
    public void testInitialize_validFile() throws Exception {
        notifierSpy.initialize(testProperties);
    }

    @Test(expected = IllegalStateException.class)
    public void testInitialize_invalidPollingPeriod() throws Exception {
        testProperties.put(FileChangeNotifier.POLLING_PERIOD_INTERVAL_KEY, "abc");
        notifierSpy.initialize(testProperties);
    }

    @Test
    public void testInitialize_useDefaultPolling() throws Exception {
        testProperties.remove(FileChangeNotifier.POLLING_PERIOD_INTERVAL_KEY);
        notifierSpy.initialize(testProperties);
    }


    @Test
    public void testNotifyListeners() throws Exception {
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        boolean wasRegistered = notifierSpy.registerListener(testListener);

        Assert.assertTrue("Registration did not correspond to newly added listener", wasRegistered);
        Assert.assertEquals("Did not receive the correct number of registered listeners", notifierSpy.getChangeListeners().size(), 1);

        notifierSpy.notifyListeners();

        verify(testListener, Mockito.atMost(1)).handleChange(Mockito.any(InputStream.class));
    }

    @Test
    public void testRegisterListener() throws Exception {
        final ConfigurationChangeListener firstListener = Mockito.mock(ConfigurationChangeListener.class);
        boolean wasRegistered = notifierSpy.registerListener(firstListener);

        Assert.assertTrue("Registration did not correspond to newly added listener", wasRegistered);
        Assert.assertEquals("Did not receive the correct number of registered listeners", notifierSpy.getChangeListeners().size(), 1);

        final ConfigurationChangeListener secondListener = Mockito.mock(ConfigurationChangeListener.class);
        wasRegistered = notifierSpy.registerListener(secondListener);
        Assert.assertEquals("Did not receive the correct number of registered listeners", notifierSpy.getChangeListeners().size(), 2);

    }

    @Test
    public void testRegisterDuplicateListener() throws Exception {
        final ConfigurationChangeListener firstListener = Mockito.mock(ConfigurationChangeListener.class);
        boolean wasRegistered = notifierSpy.registerListener(firstListener);

        Assert.assertTrue("Registration did not correspond to newly added listener", wasRegistered);
        Assert.assertEquals("Did not receive the correct number of registered listeners", notifierSpy.getChangeListeners().size(), 1);

        wasRegistered = notifierSpy.registerListener(firstListener);

        Assert.assertEquals("Did not receive the correct number of registered listeners", notifierSpy.getChangeListeners().size(), 1);
        Assert.assertFalse("Registration did not correspond to newly added listener", wasRegistered);
    }

    /* Verify handleChange events */
    @Test
    public void testTargetChangedNoModification() throws Exception {
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);

        // In this case the WatchKey is null because there were no events found
        establishMockEnvironmentForChangeTests(testListener, null);

        verify(testListener, Mockito.never()).handleChange(Mockito.any(InputStream.class));
    }

    @Test
    public void testTargetChangedWithModificationEvent_nonConfigFile() throws Exception {
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);

        // In this case, we receive a trigger event for the directory monitored, but it was another file not being monitored
        final WatchKey mockWatchKey = createMockWatchKeyForPath("footage_not_found.yml");

        establishMockEnvironmentForChangeTests(testListener, mockWatchKey);

        notifierSpy.targetChanged();

        verify(testListener, Mockito.never()).handleChange(Mockito.any(InputStream.class));
    }

    @Test
    public void testTargetChangedWithModificationEvent() throws Exception {
        final ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);

        final WatchKey mockWatchKey = createMockWatchKeyForPath(CONFIG_FILENAME);
        // Provided as a spy to allow injection of mock objects for some tests when dealing with the finalized FileSystems class
        establishMockEnvironmentForChangeTests(testListener, mockWatchKey);

        // Invoke the method of interest
        notifierSpy.run();

        verify(mockWatchService, Mockito.atLeastOnce()).poll();
        verify(testListener, Mockito.atLeastOnce()).handleChange(Mockito.any(InputStream.class));
    }

    /* Helper methods to establish mock environment */
    private WatchKey createMockWatchKeyForPath(String configFilePath) {
        final WatchKey mockWatchKey = Mockito.mock(WatchKey.class);
        final List<WatchEvent<?>> mockWatchEvents = (List<WatchEvent<?>>) Mockito.mock(List.class);
        when(mockWatchKey.pollEvents()).thenReturn(mockWatchEvents);
        when(mockWatchKey.reset()).thenReturn(true);

        final Iterator mockIterator = Mockito.mock(Iterator.class);
        when(mockWatchEvents.iterator()).thenReturn(mockIterator);

        final WatchEvent mockWatchEvent = Mockito.mock(WatchEvent.class);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next()).thenReturn(mockWatchEvent);

        // In this case, we receive a trigger event for the directory monitored, and it was the file monitored
        when(mockWatchEvent.context()).thenReturn(Paths.get(configFilePath));
        when(mockWatchEvent.kind()).thenReturn(ENTRY_MODIFY);

        return mockWatchKey;
    }

    private void establishMockEnvironmentForChangeTests(ConfigurationChangeListener listener, final WatchKey watchKey) throws Exception {
        final boolean wasRegistered = notifierSpy.registerListener(listener);

        // Establish the file mock and its parent directory
        final Path mockConfigFilePath = Mockito.mock(Path.class);
        final Path mockConfigFileParentPath = Mockito.mock(Path.class);

        // When getting the parent of the file, get the directory
        when(mockConfigFilePath.getParent()).thenReturn(mockConfigFileParentPath);

        Assert.assertTrue("Registration did not correspond to newly added listener", wasRegistered);
        Assert.assertEquals("Did not receive the correct number of registered listeners", notifierSpy.getChangeListeners().size(), 1);

        when(mockWatchService.poll()).thenReturn(watchKey);
    }
}