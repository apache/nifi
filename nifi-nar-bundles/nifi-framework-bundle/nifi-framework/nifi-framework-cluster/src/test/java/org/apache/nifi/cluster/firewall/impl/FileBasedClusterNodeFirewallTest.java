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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileBasedClusterNodeFirewallTest {

    private FileBasedClusterNodeFirewall ipsFirewall;

    private FileBasedClusterNodeFirewall acceptAllFirewall;

    private File ipsConfig;

    private File emptyConfig;

    private File restoreDirectory;

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    private static final String NONEXISTENT_HOSTNAME = "abc";

    private static boolean badHostsDoNotResolve = false;

    /**
     * We have tests that rely on known bad host/ip parameters; make sure DNS doesn't resolve them.
     * This can be a problem i.e. on residential ISPs in the USA because the provider will often
     * wildcard match all possible DNS names in an attempt to serve advertising.
     */
    @BeforeClass
    public static void ensureBadHostsDoNotWork() {
        final InetAddress ip;
        try {
            ip = InetAddress.getByName(NONEXISTENT_HOSTNAME);
        } catch (final UnknownHostException uhe) {
            badHostsDoNotResolve = true;
        }
    }

    @Before
    public void setup() throws Exception {

        ipsConfig = new File(getClass().getResource("/org/apache/nifi/cluster/firewall/impl/ips.txt").toURI());
        emptyConfig = new File(getClass().getResource("/org/apache/nifi/cluster/firewall/impl/empty.txt").toURI());

        restoreDirectory = temp.newFolder("firewall_restore");

        ipsFirewall = new FileBasedClusterNodeFirewall(ipsConfig, restoreDirectory);
        acceptAllFirewall = new FileBasedClusterNodeFirewall(emptyConfig);
    }

    /**
     * We have two garbage lines in our test config file, ensure they didn't get turned into hosts.
     */
    @Test
    public void ensureBadDataWasIgnored() {
        assumeTrue(badHostsDoNotResolve);
        assertFalse("firewall treated our malformed data as a host. If " +
                        "`host \"bad data should be skipped\"` works locally, this test should have been " +
                        "skipped.",
                ipsFirewall.isPermissible("bad data should be skipped"));
        assertFalse("firewall treated our malformed data as a host. If " +
                        "`host \"more bad data\"` works locally, this test should have been " +
                        "skipped.",
                ipsFirewall.isPermissible("more bad data"));
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
        assumeTrue(badHostsDoNotResolve);
        assertFalse("firewall allowed host '" + NONEXISTENT_HOSTNAME + "' rather than rejecting as malformed. If `host " + NONEXISTENT_HOSTNAME + "` "
                        + "works locally, this test should have been skipped.",
                ipsFirewall.isPermissible(NONEXISTENT_HOSTNAME));
    }

    @Test
    public void testIsPermissibleWithEmptyConfig() {
        assertTrue(acceptAllFirewall.isPermissible("1.1.1.1"));
    }

    @Test
    public void testIsPermissibleWithEmptyConfigWithMalformedData() {
        assumeTrue(badHostsDoNotResolve);
        assertTrue("firewall did not allow malformed host '" + NONEXISTENT_HOSTNAME + "' under permissive configs. If " +
                        "`host " + NONEXISTENT_HOSTNAME + "` works locally, this test should have been skipped.",
                acceptAllFirewall.isPermissible(NONEXISTENT_HOSTNAME));
    }

}
