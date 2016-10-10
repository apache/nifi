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
package org.apache.nifi.cluster;

import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ZooKeeperClientConfigTest {

    @Test
    public void testEasyCase(){
        final String input = "local:1234";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals(input, cleanedInput);
    }

    @Test
    public void testValidFunkyInput(){
        final String input = "local: 1234  ";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals("local:1234", cleanedInput);
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidSingleEntry(){
        ZooKeeperClientConfig.cleanConnectString("local: 1a34  ");
    }

    @Test
    public void testSingleEntryNoPort(){
        final String input = "local";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals("local:2181", cleanedInput);
    }

    @Test
    public void testMultiValidEntry(){
        final String input = "local:1234,local:1235,local:1235,local:14952";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals(input, cleanedInput);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultiValidEntrySkipOne(){
        ZooKeeperClientConfig.cleanConnectString("local:1234,local:1235,local:12a5,local:14952");
    }

    @Test
    public void testMultiValidEntrySpacesForDays(){
        final String input = "   local   :   1234  , local:  1235,local  :1295,local:14952   ";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals("local:1234,local:1235,local:1295,local:14952", cleanedInput);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultiValidOneNonsense(){
        ZooKeeperClientConfig.cleanConnectString("   local   :   1234  , local:  1235:wack,local  :1295,local:14952   ");
    }
}
