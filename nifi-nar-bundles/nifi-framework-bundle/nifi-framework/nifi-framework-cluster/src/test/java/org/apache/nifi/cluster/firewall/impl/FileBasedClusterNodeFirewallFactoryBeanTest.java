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

import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.spring.FileBasedClusterNodeFirewallFactoryBean;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FileBasedClusterNodeFirewallFactoryBeanTest {
    private static final String PROPERTIES_SUFFIX = ".firewall.properties";

    private FileBasedClusterNodeFirewallFactoryBean factoryBean;

    private NiFiProperties properties;

    @Before
    public void setFactoryBean() {
        properties = NiFiProperties.createBasicNiFiProperties(StringUtils.EMPTY);
        factoryBean = new FileBasedClusterNodeFirewallFactoryBean();
        factoryBean.setProperties(properties);
    }

    @Test
    public void testGetObjectType() {
        final Class<ClusterNodeFirewall> objectType = factoryBean.getObjectType();
        assertEquals(ClusterNodeFirewall.class, objectType);
    }

    @Test
    public void testGetObjectClusterNodeFirewallFileNotConfigured() throws Exception {
        final ClusterNodeFirewall clusterNodeFirewall = factoryBean.getObject();
        assertNull(clusterNodeFirewall);
    }

    @Test
    public void testGetObjectClusterNodeFirewallFileConfigured() throws Exception {
        final File firewallProperties = File.createTempFile(FileBasedClusterNodeFirewallFactoryBeanTest.class.getName(), PROPERTIES_SUFFIX);
        firewallProperties.deleteOnExit();

        final Map<String, String> beanProperties = new HashMap<>();
        beanProperties.put(NiFiProperties.CLUSTER_FIREWALL_FILE, firewallProperties.getAbsolutePath());
        properties = NiFiProperties.createBasicNiFiProperties(StringUtils.EMPTY, beanProperties);
        factoryBean.setProperties(properties);

        final ClusterNodeFirewall clusterNodeFirewall = factoryBean.getObject();
        assertNotNull(clusterNodeFirewall);
        assertEquals(FileBasedClusterNodeFirewall.class, clusterNodeFirewall.getClass());
    }
}
