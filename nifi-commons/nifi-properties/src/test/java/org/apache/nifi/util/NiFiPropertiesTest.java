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
package org.apache.nifi.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NiFiPropertiesTest {

    @Test
    public void testProperties() {

        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.properties");

        assertEquals("UI Banner Text", properties.getBannerText());

        Set<File> expectedDirectories = new HashSet<>();
        expectedDirectories.add(new File("./target/resources/NiFiProperties/lib/"));
        expectedDirectories.add(new File("./target/resources/NiFiProperties/lib2/"));

        Set<String> directories = new HashSet<>();
        for (Path narLibDir : properties.getNarLibraryDirectories()) {
            directories.add(narLibDir.toString());
        }

        Assert.assertEquals("Did not have the anticipated number of directories", expectedDirectories.size(), directories.size());
        for (File expectedDirectory : expectedDirectories) {
            Assert.assertTrue("Listed directories did not contain expected directory", directories.contains(expectedDirectory.getPath()));
        }
    }

    @Test
    public void testMissingProperties() {

        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.missing.properties");

        List<Path> directories = properties.getNarLibraryDirectories();

        assertEquals(1, directories.size());

        assertEquals(new File(NiFiProperties.DEFAULT_NAR_LIBRARY_DIR).getPath(), directories.get(0)
                .toString());

    }

    @Test
    public void testBlankProperties() {

        NiFiProperties properties = loadNiFiProperties("/NiFiProperties/conf/nifi.blank.properties");

        List<Path> directories = properties.getNarLibraryDirectories();

        assertEquals(1, directories.size());

        assertEquals(new File(NiFiProperties.DEFAULT_NAR_LIBRARY_DIR).getPath(), directories.get(0)
                .toString());

    }

    private NiFiProperties loadNiFiProperties(final String propsPath) {
        String realPath = null;
        try {
            realPath = NiFiPropertiesTest.class.getResource(propsPath).toURI().getPath();
        } catch (final URISyntaxException ex) {
            throw new RuntimeException(ex);
        }
        return NiFiProperties.createBasicNiFiProperties(realPath, null);
    }

}
