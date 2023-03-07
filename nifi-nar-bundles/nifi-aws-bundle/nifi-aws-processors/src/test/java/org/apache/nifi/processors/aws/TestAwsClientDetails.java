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
package org.apache.nifi.processors.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.junit.jupiter.api.Test;

public class TestAwsClientDetails {

    @Test
    public void clientDetailsEqual() {
        final AwsClientDetails details1 = getDefaultClientDetails(Regions.US_WEST_2);
        final AwsClientDetails details2 = getDefaultClientDetails(Regions.US_WEST_2);

        assertEquals(details1, details2);
        assertEquals(details1.hashCode(), details2.hashCode());
    }

    @Test
    public void clientDetailsDifferInRegion() {
        final AwsClientDetails details1 = getDefaultClientDetails(Regions.US_WEST_2);
        final AwsClientDetails details2 = getDefaultClientDetails(Regions.US_EAST_1);

        assertNotEquals(details1, details2);
        assertNotEquals(details1.hashCode(), details2.hashCode());
    }

    private AwsClientDetails getDefaultClientDetails(Regions region) {
        return new AwsClientDetails(Region.getRegion(region));
    }
}
