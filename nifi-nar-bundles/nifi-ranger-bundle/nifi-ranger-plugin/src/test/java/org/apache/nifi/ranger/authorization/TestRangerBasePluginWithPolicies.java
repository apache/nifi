/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.ranger.authorization;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class TestRangerBasePluginWithPolicies {

    @Test
    public void testDoesPolicyExist() {
        final String resourceIdentifier1 = "resource1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);

        final String resourceIdentifier2 = "resource2";
        RangerPolicy.RangerPolicyResource resource2 = new RangerPolicy.RangerPolicyResource(resourceIdentifier2);

        final Map<String, RangerPolicy.RangerPolicyResource> policy2Resources = new HashMap<>();
        policy2Resources.put(resourceIdentifier2, resource2);

        final RangerPolicy policy2 = new RangerPolicy();
        policy2.setResources(policy2Resources);

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);
        policies.add(policy2);

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi", "nifi");
        pluginWithPolicies.setPolicies(servicePolicies);

        Assert.assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier1));
        Assert.assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier2));
        Assert.assertFalse(pluginWithPolicies.doesPolicyExist("resource3"));
    }

}
