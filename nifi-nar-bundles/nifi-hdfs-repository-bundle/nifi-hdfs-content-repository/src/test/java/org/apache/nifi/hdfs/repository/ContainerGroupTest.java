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
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_CONTENT_PREFIX;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;

import static org.apache.nifi.hdfs.repository.PropertiesBuilder.SECTIONS_PER_CONTAINER;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.config;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.prop;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.props;

public class ContainerGroupTest {

    @Test
    public void noCoreSiteTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "/tmp/test-repo")
        );

        try {
            new ContainerGroup(props, config(props), null, null);
            fail("Expected container group creation to fail due to lack of core-site.xml in properties");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("No core.site.xml defined"));
        }
    }

    @Test
    public void noContainersTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        try {
            new ContainerGroup(props, config(props), Collections.singleton("disk3"), null);
            fail("Expected container group creation to fail. The group should be empty because the 'disk3' container is not defined");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("No containers found with an id of: disk3"));
        }
    }

    @Test
    public void noContainersAtAllTest() {
        NiFiProperties props = props(
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        try {
            new ContainerGroup(props, config(props), null, null);
            fail("Expected container group creation to fail becaues there are none defined");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("No content repository containers/directories specified"));
        }
    }

    @Test
    public void missingContainersFromGroupTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        try {
            new ContainerGroup(props, config(props), new HashSet<String>(Arrays.asList("disk2", "disk3")), null);
            fail("Expected container group creation to fail because the 'disk3' container is not defined");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("The following container ids should have existed, but were not found: disk3"));
        }
    }

    @Test
    public void noContainersExclusionTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        try {
            new ContainerGroup(props, config(props), null, new TreeSet<String>(Arrays.asList("disk1", "disk2")));
            fail("Expected container group creation to fail. The group should be empty because the only defined containers (disk1, and disk2) are exlcuded");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("No content repository containers specified whose id isn't one of: disk1, disk2"));
        }
    }

    @Test
    public void specificIdsTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), Collections.singleton("disk2"), null);

        assertEquals(1, group.getNumContainers());
        assertEquals(1, group.getAll().size());
        assertNotNull(group.get("disk2"));
    }

    @Test
    public void excludeIdsTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, Collections.singleton("disk2"));

        assertEquals(1, group.getNumContainers());
        assertEquals(1, group.getAll().size());
        assertNotNull(group.get("disk1"));
    }

    @Test
    public void containerCreationFailureTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "/this/path/cant/exist"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );


        try {
            new ContainerGroup(props, config(props), null, null);
            fail("Expected container group creation to fail because the directory can't be created");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("Could not create content repository directory"));
        }
    }

    @Test
    public void generalTest() throws IOException {

        String workPath = new File(".").getCanonicalPath();

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", workPath + "/target/test-repo2"),
            prop(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "50%"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);
        assertEquals(2, group.getNumContainers());
        assertNotNull(group.get("disk1"));
        assertNotNull(group.get("disk2"));
        assertEquals(2, group.getAll().size());
        assertNotNull(group.getAll().get("disk1"));
        assertNotNull(group.getAll().get("disk2"));
        assertEquals(group.get("disk1").getConfig(), group.get("disk2").getConfig());

        assertNotNull(group.atModIndex(0));
        assertNotNull(group.atModIndex(1));
        assertNotEquals(group.atModIndex(0), group.atModIndex(1));
        assertEquals(group.atModIndex(0), group.atModIndex(2));
        assertEquals(group.atModIndex(1), group.atModIndex(3));
        assertNotEquals(group.atModIndex(1), group.atModIndex(2));

        assertNotNull(group.nextActiveAtModIndex(0));
        assertNotNull(group.nextActiveAtModIndex(1));
        assertNotEquals(group.nextActiveAtModIndex(0), group.nextActiveAtModIndex(1));
        assertEquals(group.nextActiveAtModIndex(0), group.nextActiveAtModIndex(2));
        assertEquals(group.nextActiveAtModIndex(1), group.nextActiveAtModIndex(3));
        assertNotEquals(group.nextActiveAtModIndex(1), group.nextActiveAtModIndex(2));

        Container disk1 = group.get("disk1");
        assertEquals("disk1", disk1.getName());
        assertEquals("file:" + workPath + "/target/test-repo1", disk1.getPath().toString());
        assertTrue(disk1.isActive());

        Container disk2 = group.get("disk2");
        assertEquals("disk2", disk2.getName());
        assertEquals(workPath + "/target/test-repo2", disk2.getPath().toString());
        assertTrue(disk2.isActive());

        File workDir = new File(".");
        long totalSpace = workDir.getTotalSpace();
        long expFullThresh = (long)((double)totalSpace * 0.95d);
        long expMinArchive = (long)((double)totalSpace * 0.52d);

        assertEquals(expFullThresh, disk1.getFullThreshold());
        assertEquals(expMinArchive, disk1.getMinUsableSpaceForArchive());
    }

    @Test
    public void verifyDirectoryStructure() throws IOException {
        // make sure the repo directories don't currently exist;
        FileUtils.deleteDirectory(new File("target/test-repo1"));
        FileUtils.deleteDirectory(new File("target/test-repo2"));

        String workPath = new File(".").getCanonicalPath();

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", workPath + "/target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);
        assertEquals(2, group.getNumContainers());

        for (int i = 1; i <= 2; i++) {
            File repoDir = new File("target/test-repo" + i);
            assertTrue(repoDir.isDirectory());

            File[] sections = repoDir.listFiles();
            Map<String, File> sectionMap = new HashMap<>();
            for (File section : sections) {
                sectionMap.put(section.getName(), section);
            }

            assertEquals(SECTIONS_PER_CONTAINER, sections.length);

            for (int s = 0; s < SECTIONS_PER_CONTAINER; s++) {
                File section = sectionMap.get("" + s);
                assertNotNull(section);
                File[] subDirs = section.listFiles();
                assertEquals(1, subDirs.length);
                assertEquals(ARCHIVE_DIR_NAME, subDirs[0].getName());
            }
        }
    }

    @Test
    public void specificCoreSiteTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY + ".disk1", "src/test/resources/empty-core-site.xml"),
            prop(CORE_SITE_DEFAULT_PROPERTY + ".disk2", "src/test/resources/repl2-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);

        Container one = group.atModIndex(0);
        Container two = group.atModIndex(1);

        // sanity check
        assertNotEquals(one, two);

        // can't really check too much here, but we have changed the
        // replication amount, and that should affect size based properties
        assertNotEquals(one.getConfig(), two.getConfig());
        assertNotEquals(one.getConfig().get(DFSConfigKeys.DFS_REPLICATION_KEY), two.getConfig().get(DFSConfigKeys.DFS_REPLICATION_KEY));
        assertNotEquals(one.getMinUsableSpaceForArchive(), two.getMinUsableSpaceForArchive());
        assertNotEquals(one.getFullThreshold(), two.getFullThreshold());
    }

    @Test
    public void noActiveContainersTest() {
        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);

        assertEquals(2, group.getNumContainers());

        // make the first container 'full' and therefore inactive
        group.atModIndex(0).setFull(true);

        // we should still get one active group back,
        // but it will be the same for all indexes
        assertNotNull(group.nextActiveAtModIndex(0));
        assertNotNull(group.nextActiveAtModIndex(1));
        assertEquals(group.nextActiveAtModIndex(0), group.nextActiveAtModIndex(1));

        // now make the othe container 'full' too - now both containers should be inactive
        group.atModIndex(1).setFull(true);

        // we don't care whether or not the containers are active
        // while using 'atModIndex', so both should be returned
        assertNotNull(group.atModIndex(0));
        assertNotNull(group.atModIndex(1));
        assertNotEquals(group.atModIndex(0), group.atModIndex(1));

        // when we are specifically asking for the next active now,
        // we shouldn't get anything back because both containers are inactive
        assertNull(group.nextActiveAtModIndex(0));
        assertNull(group.nextActiveAtModIndex(1));
    }
}
