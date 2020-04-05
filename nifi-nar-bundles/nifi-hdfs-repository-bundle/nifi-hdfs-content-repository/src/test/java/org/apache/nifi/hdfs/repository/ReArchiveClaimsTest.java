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
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.config;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.prop;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.props;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_CONTENT_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReArchiveClaimsTest {

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void setup() {
        HdfsContentRepositoryTest.cleanRepo(new File("target/rearchive-test/primary"));
        HdfsContentRepositoryTest.cleanRepo(new File("target/rearchive-test/archive"));
    }

    @Test
    public void normalTest() throws IOException {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/rearchive-test/primary"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/rearchive-test/archive"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup primary = new ContainerGroup(props, config(props), Collections.singleton("disk1"), null);
        ContainerGroup archive = new ContainerGroup(props, config(props), Collections.singleton("disk2"), null);

        File original = new File("target/rearchive-test/primary/0/archive/claim-0");
        try (PrintWriter writer = new PrintWriter(original)) {
            writer.print("test claim data!");
        }

        assertTrue(original.isFile());

        ReArchiveClaims rearchive = new ReArchiveClaims(primary.atModIndex(0), archive, 10);

        rearchive.run();

        assertFalse(original.isFile());

        File copied = new File("target/rearchive-test/archive/0/archive/claim-0");

        assertTrue(copied.isFile());

        assertEquals("test claim data!", FileUtils.readFileToString(copied, StandardCharsets.UTF_8));
    }

    @Test
    public void failureTest() throws IOException {

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/rearchive-test/primary"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/rearchive-test/archive"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        // make sure the original archive file remains even if copying fails
        ContainerGroup primary = new ContainerGroup(props, config(props), Collections.singleton("disk1"), null);
        ContainerGroup archive = new ContainerGroup(props, config(props), Collections.singleton("disk2"), null) {
            @Override
            public Container atModIndex(long index) {
                try {
                    return new Container(null, null, null, index, index, false) {
                        @Override
                        public FileSystem getFileSystem() throws IOException {
                            throw new IOException("Error thrown for testing purposes!");
                        }
                    };
                } catch (IOException ex)  {
                    throw new RuntimeException(ex);
                }
            }
        };

        File original = new File("target/rearchive-test/primary/0/archive/claim-0");
        try (PrintWriter writer = new PrintWriter(original)) {
            writer.print("test claim data!");
        }

        ReArchiveClaims rearchive = new ReArchiveClaims(primary.atModIndex(0), archive, 10);

        // rearchiving should fail because we throw an exception when getFileSystem on the archive container
        rearchive.run();

        File copied = new File("target/rearchive-test/archive/0/archive/claim-0");

        // should still exist
        assertTrue(original.isFile());

        // shouldn't exist since the rearchive failed
        assertFalse(copied.isFile());
    }
}
