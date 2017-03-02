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
package org.apache.nifi.registry;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestVariableRegistry {

    @Test
    public void testSystemProp() {
        assertNull(System.getProperty("ALKJAFLKJDFLSKJSDFLKJSDF"));
        final VariableRegistry sysEvnReg = VariableRegistry.ENVIRONMENT_SYSTEM_REGISTRY;
        System.setProperty("ALKJAFLKJDFLSKJSDFLKJSDF", "here now");
        //should not be in Variable Registry
        assertNull(sysEvnReg.getVariableValue("ALKJAFLKJDFLSKJSDFLKJSDF"));
        //should be in System properties now though...
        assertEquals("here now", System.getProperty("ALKJAFLKJDFLSKJSDFLKJSDF"));

        //Test should be stable but a security manager could block it.  The following assertions are optional and based on access to the following property.
        //It was chosen from this list https://docs.oracle.com/javase/tutorial/essential/environment/sysprop.html
        final String vendorUrl = System.getProperty("java.vendor.url");
        if (vendorUrl != null) { // we can run this extra test
            //var reg value matches system property
            assertEquals(vendorUrl, sysEvnReg.getVariableValue("java.vendor.url"));
            //change system property
            System.setProperty("java.vendor.url", "http://fake.vendor.url/");
            //changed in system properties
            assertEquals("http://fake.vendor.url/", System.getProperty("java.vendor.url"));
            //var reg value matches system property still
            assertEquals(vendorUrl, sysEvnReg.getVariableValue("java.vendor.url"));
            //restore to its old value
            System.setProperty("java.vendor.url", vendorUrl);
        }
    }

}
