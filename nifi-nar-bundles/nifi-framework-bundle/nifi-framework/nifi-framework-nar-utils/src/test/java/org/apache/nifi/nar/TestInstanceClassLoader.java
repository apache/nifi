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
package org.apache.nifi.nar;

import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class TestInstanceClassLoader {

    @Test
    public void testFindResourceWhenFile() throws MalformedURLException {
        final File nifiProperties = new File("src/test/resources/nifi.properties");
        assertTrue(nifiProperties.exists());

        final URL nifiPropertiesURL = nifiProperties.toURI().toURL();

        final ClassLoader instanceClassLoader = new InstanceClassLoader(
                "id",
                "org.apache.nifi.processors.MyProcessor",
                Collections.emptySet(),
                Collections.singleton(nifiPropertiesURL),
                null);

        final URL nifiPropertiesResource = instanceClassLoader.getResource(nifiProperties.getName());
        assertNotNull(nifiPropertiesResource);
        assertEquals(nifiPropertiesURL.toExternalForm(), nifiPropertiesResource.toExternalForm());

        final URL doesNotExistResource = instanceClassLoader.getResource("does-not-exist.txt");
        assertNull(doesNotExistResource);
    }
}
