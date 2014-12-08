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
package org.apache.nifi.cluster.firewall.impl;

import org.apache.nifi.cluster.firewall.impl.FileBasedClusterNodeFirewall;
import java.io.File;
import java.io.IOException;
import org.apache.nifi.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Test;

public class FileBasedClusterNodeFirewallTest {

    private FileBasedClusterNodeFirewall ipsFirewall;

    private FileBasedClusterNodeFirewall acceptAllFirewall;

    private File ipsConfig;

    private File emptyConfig;

    private File restoreDirectory;

    @Before
    public void setup() throws Exception {

        ipsConfig = new File("src/test/resources/org/apache/nifi/cluster/firewall/impl/ips.txt");
        emptyConfig = new File("src/test/resources/org/apache/nifi/cluster/firewall/impl/empty.txt");

        restoreDirectory = new File(System.getProperty("java.io.tmpdir") + "/firewall_restore");

        ipsFirewall = new FileBasedClusterNodeFirewall(ipsConfig, restoreDirectory);
        acceptAllFirewall = new FileBasedClusterNodeFirewall(emptyConfig);
    }

    @After
    public void teardown() throws IOException {
        deleteFile(restoreDirectory);
    }

    @Test
    public void testSyncWithRestore() {
        assertEquals(ipsConfig.length(), new File(restoreDirectory, ipsConfig.getName()).length());
    }

    @Test
    public void testIsPermissibleWithExactMatch() {
        assertTrue(ipsFirewall.isPermissible("2.2.2.2"));
    }

    @Test
    public void testIsPermissibleWithSubnetMatch() {
        assertTrue(ipsFirewall.isPermissible("3.3.3.255"));
    }

    @Test
    public void testIsPermissibleWithNoMatch() {
        assertFalse(ipsFirewall.isPermissible("255.255.255.255"));
    }

    @Test
    public void testIsPermissibleWithMalformedData() {
        assertFalse(ipsFirewall.isPermissible("abc"));
    }

    @Test
    public void testIsPermissibleWithEmptyConfig() {
        assertTrue(acceptAllFirewall.isPermissible("1.1.1.1"));
    }

    @Test
    public void testIsPermissibleWithEmptyConfigWithMalformedData() {
        assertTrue(acceptAllFirewall.isPermissible("abc"));
    }

    private boolean deleteFile(final File file) {
        if (file.isDirectory()) {
            FileUtils.deleteFilesInDir(file, null, null, true, true);
        }
        return FileUtils.deleteFile(file, null, 10);
    }

}
