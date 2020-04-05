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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class BinDestructableClaimsTest {

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Test
    public void binningTest() throws Exception {

        //
        // Okay, this is gross...
        //
        // This test is doing the following:
        //
        // 1. setup a mock ResourceClaimManager
        // 2. make some fake claims
        // 3. add the fake claims to the claim manager
        // 4. manually run the binner
        // 5. verify they're all binned properly
        //

        // this is the go-between for the claim manager and the test
        ArrayBlockingQueue<ResourceClaim> claims = new ArrayBlockingQueue<>(1000);

        // craete a mock claim manager
        InvocationHandler handler = new InvocationHandler(){
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                @SuppressWarnings("unchecked")
                Collection<ResourceClaim> drainTo = (Collection<ResourceClaim>)args[0];
                int maxElements = Math.min((int)args[1], claims.size());
                claims.drainTo(drainTo, maxElements);
                return null;
            }
        };
        ResourceClaimManager claimManager = (ResourceClaimManager)Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[] { ResourceClaimManager.class },
            handler
        );

        NiFiProperties props = props(
            prop(REPOSITORY_CONTENT_PREFIX + "disk1", "target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml")
        );

        ContainerGroup group = new ContainerGroup(props, config(props), null, null);

        BinDestructableClaims binner = new BinDestructableClaims(claimManager, group.getAll());

        // make some fake claims and 'add' them to the claim manager
        for (int i = 0; i < 100; i++) {
            claims.add(makeClaim(group.atModIndex(i).getName(), i));
        }

        binner.run();

        if (claims.size() > 0) {
            fail("Binner failed to drain all claims from the claim manager - there are " + claims.size() + " of 100 left");
        }

        // give the binner some extra time to bin everything
        Thread.sleep(20);

        List<ResourceClaim> drain = new ArrayList<>();

        // verify half the claims made it into the first container
        group.atModIndex(0).drainReclaimable(drain);
        assertEquals(50, drain.size());
        int expIndex = 0;
        for (ResourceClaim claim : drain) {
            assertEquals("" + expIndex, claim.getId());
            expIndex += 2;
        }

        drain.clear();

        // verify the other half made it into the second container
        group.atModIndex(1).drainReclaimable(drain);
        assertEquals(50, drain.size());
        expIndex = 1;
        for (ResourceClaim claim : drain) {
            assertEquals("" + expIndex, claim.getId());
            expIndex += 2;
        }
    }

    protected static ResourceClaim makeClaim(String container, int index) {
        ResourceClaim claim = mock(ResourceClaim.class);
        when(claim.getId()).thenReturn("" + index);
        when(claim.getContainer()).thenReturn(container);
        return claim;
    }
}
