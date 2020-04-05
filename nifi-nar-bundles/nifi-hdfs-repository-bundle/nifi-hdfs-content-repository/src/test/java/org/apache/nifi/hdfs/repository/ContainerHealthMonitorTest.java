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
package org.apache.nifi.hdfs.repository;

import static org.apache.nifi.hdfs.repository.HdfsContentRepository.CORE_SITE_DEFAULT_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FAILURE_TIMEOUT_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FULL_PERCENTAGE_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.OPERATING_MODE_PROPERTY;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.config;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.prop;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.props;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_CONTENT_PREFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Ignore;
import org.junit.Test;

public class ContainerHealthMonitorTest {

    @Test
    public void isGroupHealthyTest() {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(FAILURE_TIMEOUT_PROPERTY, "1 minute")
        );
        RepositoryConfig config = config(props);

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(null, null, null, config);

        // null groups should always return true since this is only
        // called for the secondary group, and the return value doesn't matter
        assertTrue(monitor.isGroupHealthy(null));

        ContainerGroup group = new ContainerGroup(props, config, null, null);

        assertTrue(monitor.isGroupHealthy(group));

        // all failed test
        for (Container container : group) {
            container.failureOcurred();
        }

        // since all containers are failed, the group is not healthy
        assertFalse(monitor.isGroupHealthy(group));
        for (Container container : group) {
            assertTrue(container.isFailedRecently());
        }

        // partial failure test - one container is failed, the other is not, so overall
        // the group is healthy
        group.atModIndex(0).clearFailure(group.atModIndex(0).getLastFailure());
        assertTrue(monitor.isGroupHealthy(group));

    }

    @Test
    public void isGroupHealthyRecoveryTest() throws IOException {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(FAILURE_TIMEOUT_PROPERTY, "1 minute")
        );
        RepositoryConfig config = config(props);

        ContainerGroup group = new ContainerGroup(props, config, null, null) {

            TimeAdjustingFailureContainer indexOneContainer = null;

            @Override
            public Iterator<Container> iterator() {
                if (indexOneContainer == null) {
                    try {
                        indexOneContainer = new TimeAdjustingFailureContainer(atModIndex(1));
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                return Arrays.asList(atModIndex(0), indexOneContainer).iterator();
            }
        };

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(null, null, null, config);

        assertTrue(monitor.isGroupHealthy(group));

        for (Container container : group) {
            container.failureOcurred();
            assertTrue(container.isFailedRecently());
        }

        // the group should still be healthy and the one
        // container should now be marked as not recently failed
        assertTrue(monitor.isGroupHealthy(group));

        Iterator<Container> containerIter = group.iterator();
        Container one = containerIter.next();
        assertFalse(one.isActive());
        assertTrue(one.isFailedRecently());

        Container two = containerIter.next();
        assertTrue(two.isActive());
        assertFalse(two.isFailedRecently());
    }

    @Test
    public void checkDiskHealthTest() throws IOException {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(FULL_PERCENTAGE_PROPERTY, "99%")
        );
        RepositoryConfig config = config(props);

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(null, null, null, config);

        // null groups should always return true since this is only
        // called for the secondary group, and the return value doesn't matter
        assertTrue(monitor.doesGroupHaveFreeSpace(null));

        ContainerGroup realGroup = new ContainerGroup(props, config, null, null);

        assertTrue(monitor.doesGroupHaveFreeSpace(realGroup));

        //
        // get the file system to say it's full
        //

        Container realContainer = realGroup.atModIndex(0);
        FileSystem realFs = realContainer.getFileSystem();
        FsStatus realStatus = realFs.getStatus(realContainer.getPath());

        Container fullContainer = spy(realContainer);
        FileSystem fullFs = spy(realFs);
        FsStatus fullStatus = spy(realStatus);

        when(fullStatus.getUsed()).thenReturn(realStatus.getCapacity());
        when(fullStatus.getRemaining()).thenReturn(0L);
        when(fullFs.getStatus(any(Path.class))).thenReturn(fullStatus);
        when(fullContainer.getFileSystem()).thenReturn(fullFs);

        ContainerGroup oneFullGroup = spy(realGroup);
        when(oneFullGroup.iterator()).thenReturn(Arrays.asList(fullContainer, realGroup.atModIndex(1)).iterator());

        assertFalse(fullContainer.isFull());
        assertTrue(fullContainer.isActive());

        // the group should still have space because one of the containers is
        // not full. Make sure the 'full' container is now marked as full
        assertTrue(monitor.doesGroupHaveFreeSpace(oneFullGroup));
        assertTrue(fullContainer.isFull());
        assertFalse(fullContainer.isActive());

        // reset the full container's state
        fullContainer.setFull(false);
        assertFalse(fullContainer.isFull());
        assertTrue(fullContainer.isActive());

        // now get the other container to say it's full as well
        ContainerGroup completelyFull = spy(realGroup);
        Container fullContainerTwo = spy(realGroup.atModIndex(1));
        when(fullContainerTwo.getFileSystem()).thenReturn(fullFs);
        when(completelyFull.iterator()).thenReturn(Arrays.asList(fullContainer, fullContainerTwo).iterator());

        // since both containers are full, there should no longer be any free space
        assertFalse(monitor.doesGroupHaveFreeSpace(completelyFull));

        // both containers should be marked full and inactive
        for (Container container : completelyFull) {
            assertTrue(container.isFull());
            assertFalse(container.isActive());
        }
    }

    @Test
    public void switchActiveTest() {

        //
        // Make sure the monitor switches between primary/secondary
        // properly with each subsequent call to switchActive()
        //

        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        ContainerGroup primary = mock(ContainerGroup.class);
        ContainerGroup secondary = mock(ContainerGroup.class);

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(repo, primary, secondary, config(props()));

        monitor.switchActive("test");

        verify(repo, times(1)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(1)).setActiveGroup(eq(secondary));

        monitor.switchActive("test");

        verify(repo, times(2)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(1)).setActiveGroup(eq(secondary));
        verify(repo, times(1)).setActiveGroup(eq(primary));

        monitor.switchActive("test");

        verify(repo, times(3)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(2)).setActiveGroup(eq(secondary));
        verify(repo, times(1)).setActiveGroup(eq(primary));

        monitor.switchActive("test");

        verify(repo, times(4)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(2)).setActiveGroup(eq(secondary));
        verify(repo, times(2)).setActiveGroup(eq(primary));
    }

    @Test
    public void capacityFallbackTest() {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        ContainerGroup primary = mock(ContainerGroup.class);
        ContainerGroup secondary = mock(ContainerGroup.class);

        RepositoryConfig config = config(props(
            prop(OPERATING_MODE_PROPERTY, "CapacityFallback")
        ));

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(repo, primary, secondary, config);
        monitor = spy(monitor);

        when(monitor.doesGroupHaveFreeSpace(any(ContainerGroup.class))).thenReturn(
            true, true,     // CALL #1
            false, false,   // CALL #2
            false, false,   // CALL #3
            true, true      // CALL #4
        );
        when(monitor.isGroupHealthy(any(ContainerGroup.class))).thenReturn(true);

        // CALL #1
        // first run, the primary group will have free space, so no switch should happen
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));
        verify(monitor, times(1)).doesGroupHaveFreeSpace(eq(primary));
        verify(monitor, times(1)).doesGroupHaveFreeSpace(eq(secondary));

        // CALL #2
        // now run it again, and primary should be full, so we should switch to the secondary
        monitor.run();
        verify(repo, times(1)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(1)).setActiveGroup(eq(secondary));
        verify(monitor, times(2)).doesGroupHaveFreeSpace(eq(primary));
        verify(monitor, times(2)).doesGroupHaveFreeSpace(eq(secondary));

        // CALL #3
        // running again should have no affect since the primary is still full
        monitor.run();
        verify(repo, times(1)).setActiveGroup(any(ContainerGroup.class));
        verify(monitor, times(3)).doesGroupHaveFreeSpace(eq(primary));
        verify(monitor, times(3)).doesGroupHaveFreeSpace(eq(secondary));

        // CALL #4
        // running one last time, primary should no longer be full, and should be set to active again
        monitor.run();
        verify(repo, times(2)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(1)).setActiveGroup(eq(secondary));
        verify(repo, times(1)).setActiveGroup(eq(primary));
        verify(monitor, times(4)).doesGroupHaveFreeSpace(eq(primary));
        verify(monitor, times(4)).doesGroupHaveFreeSpace(eq(secondary));

    }

    @Test
    public void failureFallbackTest() {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        ContainerGroup primary = mock(ContainerGroup.class);
        ContainerGroup secondary = mock(ContainerGroup.class);

        RepositoryConfig config = config(props(
            prop(OPERATING_MODE_PROPERTY, "FailureFallback"),
            prop(FAILURE_TIMEOUT_PROPERTY, "1 minute")
        ));

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(repo, primary, secondary, config);
        monitor = spy(monitor);

        when(monitor.isGroupHealthy(any(ContainerGroup.class))).thenReturn(
            true, true,     // CALL #1
            false, false,   // CALL #2
            false, false,   // CALL #3
            true, true      // CALL #4
        );
        when(monitor.doesGroupHaveFreeSpace(any(ContainerGroup.class))).thenReturn(true);

        // CALL #1
        // first run, the primary group will be healthy, so no switch should happen
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));
        verify(monitor, times(1)).isGroupHealthy(eq(primary));
        verify(monitor, times(1)).isGroupHealthy(eq(secondary));

        // CALL #2
        // now run it again, and primary should be unhealthy, so we should switch to the secondary
        monitor.run();
        verify(repo, times(1)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(1)).setActiveGroup(eq(secondary));
        verify(monitor, times(2)).isGroupHealthy(eq(primary));
        verify(monitor, times(2)).isGroupHealthy(eq(secondary));

        // CALL #3
        // running again should have no affect since the primary is still unhealthy
        monitor.run();
        verify(repo, times(1)).setActiveGroup(any(ContainerGroup.class));
        verify(monitor, times(3)).isGroupHealthy(eq(primary));
        verify(monitor, times(3)).isGroupHealthy(eq(secondary));

        // CALL #4
        // running one last time, primary should be healthy again, and should be set to active again
        monitor.run();
        verify(repo, times(2)).setActiveGroup(any(ContainerGroup.class));
        verify(repo, times(1)).setActiveGroup(eq(secondary));
        verify(repo, times(1)).setActiveGroup(eq(primary));
        verify(monitor, times(4)).isGroupHealthy(eq(primary));
        verify(monitor, times(4)).isGroupHealthy(eq(secondary));
    }

    @Test
    public void monitorOnlyFullTest() {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        ContainerGroup primary = mock(ContainerGroup.class);
        ContainerGroup secondary = mock(ContainerGroup.class);

        RepositoryConfig config = config(props());

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(repo, primary, secondary, config);
        monitor = spy(monitor);

        when(monitor.isGroupHealthy(any(ContainerGroup.class))).thenReturn(false);
        when(monitor.doesGroupHaveFreeSpace(any(ContainerGroup.class))).thenReturn(true);

        // since fallback isn't enabled, no switching should happen even though primary is full
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));

        // double check
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));
    }

    @Test
    public void monitorOnlyFailureTest() {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        ContainerGroup primary = mock(ContainerGroup.class);
        ContainerGroup secondary = mock(ContainerGroup.class);

        RepositoryConfig config = config(props());

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(repo, primary, secondary, config);
        monitor = spy(monitor);

        when(monitor.isGroupHealthy(any(ContainerGroup.class))).thenReturn(true);
        when(monitor.doesGroupHaveFreeSpace(any(ContainerGroup.class))).thenReturn(false);

        // since fallback isn't enabled, no switching should happen even though primary is unhealthy
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));

        // double check
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));
    }

    @Test
    public void monitorOnlyBothTest() {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        ContainerGroup primary = mock(ContainerGroup.class);
        ContainerGroup secondary = mock(ContainerGroup.class);

        RepositoryConfig config = config(props());

        ContainerHealthMonitor monitor = new ContainerHealthMonitor(repo, primary, secondary, config);
        monitor = spy(monitor);

        when(monitor.isGroupHealthy(any(ContainerGroup.class))).thenReturn(false);
        when(monitor.doesGroupHaveFreeSpace(any(ContainerGroup.class))).thenReturn(false);

        // since fallback isn't enabled, no switching should happen even though primary is unhealthy and full
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));

        // double check
        monitor.run();
        verify(repo, times(0)).setActiveGroup(any(ContainerGroup.class));
    }

    @Ignore
    private static class TimeAdjustingFailureContainer extends Container {

        private final Container delegate;
        private long useLastFailureTime = 0;

        public TimeAdjustingFailureContainer(Container delegate) throws IOException {
            super("test-adjust", new Path("file:/this/path/doesnt/exist"), null, 0, 0, true);
            this.delegate = delegate;
        }

        @Override
        public void failureOcurred() {
            delegate.failureOcurred();
            useLastFailureTime = System.currentTimeMillis() - 1000 * 60 * 60 * 60;
        }

        @Override
        public long getLastFailure() {
            return useLastFailureTime;
        }

        @Override
        public boolean isActive() {
            return delegate.isActive();
        }

        @Override
        public boolean isFailedRecently() {
            return delegate.isFailedRecently();
        }

        @Override
        public synchronized boolean clearFailure(long expectedLastFailure) {
            if (expectedLastFailure == useLastFailureTime) {
                assertTrue(delegate.clearFailure(delegate.getLastFailure()));
                return true;
            }
            return false;
        }

    }
}
