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

import static org.apache.nifi.hdfs.repository.HdfsContentRepository.ARCHIVE_DIR_NAME;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.CORE_SITE_DEFAULT_PROPERTY;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.config;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.prop;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.props;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_CONTENT_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;

public class DestroyExpiredArchiveClaimsTest {

    private final File base = new File("target/destroy-archive-test/disk1");

    @Before
    public void setup() throws IOException {
        HdfsContentRepositoryTest.cleanRepo(base);
    }

    @Test
    public void ageOffOnlyTest() throws IOException {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "file:target/destroy-archive-test/disk1"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "99%")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);

        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        when(repo.getContainerUsableSpace(anyString())).thenReturn(base.getUsableSpace());

        // current time without milli granularity
        long now = (System.currentTimeMillis() / 1000) * 1000;

        DestroyExpiredArchiveClaims destroy = new DestroyExpiredArchiveClaims(repo, group.iterator().next(), 30000);

        File archiveDir = new File(new File(base, "0"), ARCHIVE_DIR_NAME);

        File one = touch(archiveDir, "claim-0", now);
        File two = touch(archiveDir, "claim-1", now - 5000);
        File three = touch(archiveDir, "claim-2", now - 60000);

        // This should delete 'three' because it's more than 30 seconds
        // old. It should return 'two's time as the next oldest file that isn't aged off
        assertEquals(now - 5000, destroy.destroyExpiredArchives());

        // one and two should still exist since they were created less than 30 seconds ago
        assertTrue(one.isFile());
        assertTrue(two.isFile());
        assertFalse(three.isFile());
    }

    @Test
    public void oversizedTest() throws IOException {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "file:target/destroy-archive-test/disk1"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "50%")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);

        HdfsContentRepository repo = mock(HdfsContentRepository.class);

        // current time without milli granularity
        long now = (System.currentTimeMillis() / 1000) * 1000;

        Container container = spy(group.iterator().next());
        // with minimum size: 990, and current size: 1000, we need to free: 10 bytes
        when(container.getMinUsableSpaceForArchive()).thenReturn(1000L);
        when(repo.getContainerUsableSpace(anyString())).thenReturn(990L);

        // each file is 10 bytes big, so only one needs to be deleted
        File archiveDir = new File(new File(base, "0"), ARCHIVE_DIR_NAME);
        File one = touch(archiveDir, "claim-1", now, "0123456789");
        File two = touch(archiveDir, "claim-2", now - 10000, "0123456789");
        File three = touch(archiveDir, "claim-3", now - 9000, "0123456789");

        DestroyExpiredArchiveClaims destroy = new DestroyExpiredArchiveClaims(repo, container, 1000 * 60 * 5);

        // This should delete 'two' because the oldest file it shouldn't
        // delete 'three' or 'one' because they are not expired, and deleting
        // 'two' is sufficient to bring the free space up to the desired amount
        assertEquals(now - 9000, destroy.destroyExpiredArchives());

        // one and two should still exist since they were created less than 30 seconds ago
        assertTrue(one.isFile());
        assertFalse(two.isFile());
        assertTrue(three.isFile());
    }

    @Test
    public void noDeletionTest() throws IOException {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "file:target/destroy-archive-test/disk1"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "99%")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);

        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        when(repo.getContainerUsableSpace(anyString())).thenReturn(base.getUsableSpace());

        // current time without milli granularity
        long now = (System.currentTimeMillis() / 1000) * 1000;

        Container container = group.iterator().next();

        File archiveDir = new File(new File(base, "0"), ARCHIVE_DIR_NAME);
        File one = touch(archiveDir, "claim-1", now, "0123456789");
        File two = touch(archiveDir, "claim-2", now - 10000, "0123456789");
        File three = touch(archiveDir, "claim-3", now - 20000, "0123456789");

        DestroyExpiredArchiveClaims destroy = new DestroyExpiredArchiveClaims(repo, container, 1000 * 60 * 5);

        // this shouldn't delete anything since all 3 files are new enough,
        // and it's not possible to hit the free space requirement.
        assertEquals(now - 20000, destroy.destroyExpiredArchives());

        assertTrue(one.isFile());
        assertTrue(two.isFile());
        assertTrue(three.isFile());
    }

    @Test
    public void noActionNeededTest() throws IOException {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        Container container = mock(Container.class);
        when(container.getName()).thenReturn("test");

        // we have a 1000 byte surplus in space
        when(container.getMinUsableSpaceForArchive()).thenReturn(1000L);
        when(repo.getContainerUsableSpace(anyString())).thenReturn(2000L);

        DestroyExpiredArchiveClaims destroy = new DestroyExpiredArchiveClaims(repo, container, 1000 * 60 * 5);
        destroy = spy(destroy);
        doReturn(System.currentTimeMillis()).when(destroy).destroyExpiredArchives();

        verify(destroy, times(0)).destroyExpiredArchives();

        // first time, the process needs to figure out what the oldest file in the archive is
        destroy.run();

        verify(repo, times(0)).getContainerUsableSpace(any());
        verify(destroy, times(1)).destroyExpiredArchives();

        destroy.run();

        verify(repo, times(1)).getContainerUsableSpace(any());
        verify(destroy, times(1)).destroyExpiredArchives();
    }

    @Test
    public void usableSpaceExceededTest() throws IOException {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        Container container = mock(Container.class);
        when(container.getName()).thenReturn("test");

        // 1000 bytes need to be relcaimed
        when(container.getMinUsableSpaceForArchive()).thenReturn(2000L);
        when(repo.getContainerUsableSpace(anyString())).thenReturn(1000L);

        DestroyExpiredArchiveClaims destroy = new DestroyExpiredArchiveClaims(repo, container, 1000 * 60 * 5);
        destroy = spy(destroy);
        doReturn(System.currentTimeMillis()).when(destroy).destroyExpiredArchives();

        verify(destroy, times(0)).destroyExpiredArchives();

        // first time, the process needs to figure out what the oldest file in the archive is
        destroy.run();

        verify(repo, times(0)).getContainerUsableSpace(any());
        verify(destroy, times(1)).destroyExpiredArchives();

        destroy.run();

        verify(repo, times(1)).getContainerUsableSpace(any());
        verify(destroy, times(2)).destroyExpiredArchives();
    }

    @Test
    public void ageOffTimeExceededTest() throws IOException {
        HdfsContentRepository repo = mock(HdfsContentRepository.class);
        Container container = mock(Container.class);
        when(container.getName()).thenReturn("test");

        // we have a 1000 byte surplus in space
        when(container.getMinUsableSpaceForArchive()).thenReturn(1000L);
        when(repo.getContainerUsableSpace(anyString())).thenReturn(2000L);

        DestroyExpiredArchiveClaims destroy = new DestroyExpiredArchiveClaims(repo, container, 1000 * 60);
        destroy = spy(destroy);
        doReturn(System.currentTimeMillis() - (1000 * 60 * 2)).when(destroy).destroyExpiredArchives();

        verify(destroy, times(0)).destroyExpiredArchives();

        // first time, the process needs to figure out what the oldest file in the archive is (2 minutes ago)
        destroy.run();

        verify(repo, times(0)).getContainerUsableSpace(any());
        verify(destroy, times(1)).destroyExpiredArchives();

        destroy.run();

        // getContainerUsableSpace is never called because the archive date condition is hit instead
        verify(repo, times(0)).getContainerUsableSpace(any());
        verify(destroy, times(2)).destroyExpiredArchives();
    }

    private static File touch(File dir, String name, long time) throws IOException {
        return touch(dir, name, time, "");
    }
    private static File touch(File dir, String name, long time, String data) throws IOException {
        File file = new File(dir, name);
        FileUtils.writeStringToFile(file, data, StandardCharsets.UTF_8);
        file.setLastModified(time);
        return file;
    }
}
