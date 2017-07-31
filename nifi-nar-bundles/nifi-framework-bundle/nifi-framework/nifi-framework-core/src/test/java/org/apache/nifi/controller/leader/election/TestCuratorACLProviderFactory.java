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
package org.apache.nifi.controller.leader.election;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TestCuratorACLProviderFactory {

    private volatile String propsFile = TestCuratorACLProviderFactory.class.getResource("/flowcontrollertest.nifi.properties").getFile();
    final Map<String, String> otherProps = new HashMap<>();

    @Before
    public void setup(){
        otherProps.put("nifi.zookeeper.connect.string", "local:1234");
        otherProps.put("nifi.zookeeper.root.node", "/nifi");
        otherProps.put("nifi.zookeeper.auth.type", "sasl");
        otherProps.put("nifi.kerberos.service.principal","nifi/host@REALM.COM");
    }

    @Test
    public void testSaslAuthSchemeNoHostNoRealm(){
        final NiFiProperties nifiProperties;
        final CuratorACLProviderFactory factory;
        otherProps.put("nifi.zookeeper.kerberos.removeHostFromPrincipal", "true");
        otherProps.put("nifi.zookeeper.kerberos.removeRealmFromPrincipal", "true");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        factory = new CuratorACLProviderFactory();
        ZooKeeperClientConfig config = ZooKeeperClientConfig.createConfig(nifiProperties);
        ACLProvider provider = factory.create(config);
        assertFalse(provider instanceof DefaultACLProvider);
        List<ACL> acls = provider.getDefaultAcl();
        assertNotNull(acls);
        assertEquals(acls.get(0).getId().toString().trim(),"'sasl,'nifi");
    }

    @Test
    public void testSaslAuthSchemeHeadless(){
        final NiFiProperties nifiProperties;
        final CuratorACLProviderFactory factory;
        otherProps.put("nifi.zookeeper.kerberos.removeHostFromPrincipal", "true");
        otherProps.put("nifi.zookeeper.kerberos.removeRealmFromPrincipal", "true");
        otherProps.put("nifi.kerberos.service.principal","nifi@REALM.COM");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        factory = new CuratorACLProviderFactory();
        ZooKeeperClientConfig config = ZooKeeperClientConfig.createConfig(nifiProperties);
        ACLProvider provider = factory.create(config);
        assertFalse(provider instanceof DefaultACLProvider);
        List<ACL> acls = provider.getDefaultAcl();
        assertNotNull(acls);
        assertEquals(acls.get(0).getId().toString().trim(),"'sasl,'nifi");
    }

    @Test
    public void testSaslAuthSchemeNoHostWithRealm(){

        final NiFiProperties nifiProperties;
        final CuratorACLProviderFactory factory;
        otherProps.put("nifi.zookeeper.kerberos.removeHostFromPrincipal", "true");
        otherProps.put("nifi.zookeeper.kerberos.removeRealmFromPrincipal", "false");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        factory = new CuratorACLProviderFactory();
        ZooKeeperClientConfig config = ZooKeeperClientConfig.createConfig(nifiProperties);
        ACLProvider provider = factory.create(config);
        assertFalse(provider instanceof DefaultACLProvider);
        List<ACL> acls = provider.getDefaultAcl();
        assertNotNull(acls);
        assertEquals(acls.get(0).getId().toString().trim(),"'sasl,'nifi@REALM.COM");

    }

    @Test
    public void testSaslAuthSchemeWithHostNoRealm(){

        final NiFiProperties nifiProperties;
        final CuratorACLProviderFactory factory;
        otherProps.put("nifi.zookeeper.kerberos.removeHostFromPrincipal", "false");
        otherProps.put("nifi.zookeeper.kerberos.removeRealmFromPrincipal", "true");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        factory = new CuratorACLProviderFactory();
        ZooKeeperClientConfig config = ZooKeeperClientConfig.createConfig(nifiProperties);
        ACLProvider provider = factory.create(config);
        assertFalse(provider instanceof DefaultACLProvider);
        List<ACL> acls = provider.getDefaultAcl();
        assertNotNull(acls);
        assertEquals(acls.get(0).getId().toString().trim(),"'sasl,'nifi/host");

    }

}
