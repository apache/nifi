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
package org.apache.nifi.integration.accesscontrol.anonymous;

import org.apache.nifi.integration.accesscontrol.AccessControlHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 * Integration test for preventing proxied anonymous access.
 */
public class ITPreventProxiedAnonymousAccess extends AbstractAnonymousUserTest {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        System.out.println(ITPreventProxiedAnonymousAccess.class.getName() + " setup()");
        helper = new AccessControlHelper();
    }

    /**
     * Attempt to create a processor anonymously.
     *
     * @throws Exception ex
     */
    @Test
    public void testProxiedAnonymousAccess() throws Exception {
        final Response response = super.testCreateProcessor(helper.getBaseUrl(), helper.getAnonymousUser());

        // ensure the request is not successful
        Assert.assertEquals(401, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
