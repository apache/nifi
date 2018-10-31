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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NarThreadContextClassLoaderTest {

    @Test
    public void validateWithPropertiesConstructor() throws Exception {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties("src/test/resources/nifi.properties", null);
        Bundle systemBundle = SystemBundle.create(properties);
        ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        Object obj = NarThreadContextClassLoader.createInstance(extensionManager, WithPropertiesConstructor.class.getName(),
                WithPropertiesConstructor.class, properties);
        assertTrue(obj instanceof WithPropertiesConstructor);
        WithPropertiesConstructor withPropertiesConstructor = (WithPropertiesConstructor) obj;
        assertNotNull(withPropertiesConstructor.properties);
    }

    @Test(expected = IllegalStateException.class)
    public void validateWithPropertiesConstructorInstantiationFailure() throws Exception {
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("fail", "true");
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties("src/test/resources/nifi.properties", additionalProperties);
        Bundle systemBundle = SystemBundle.create(properties);
        ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
        NarThreadContextClassLoader.createInstance(extensionManager, WithPropertiesConstructor.class.getName(), WithPropertiesConstructor.class, properties);
    }

    @Test
    public void validateWithDefaultConstructor() throws Exception {
        NiFiProperties properties = NiFiProperties.createBasicNiFiProperties("src/test/resources/nifi.properties", null);
        Bundle systemBundle = SystemBundle.create(properties);
        ExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());
        assertTrue(NarThreadContextClassLoader.createInstance(extensionManager, WithDefaultConstructor.class.getName(),
                WithDefaultConstructor.class, properties) instanceof WithDefaultConstructor);
    }

    public static class WithPropertiesConstructor extends AbstractProcessor {
        private NiFiProperties properties;

        public WithPropertiesConstructor() {

        }

        public WithPropertiesConstructor(NiFiProperties properties) {
            if (properties.getProperty("fail") != null) {
                throw new RuntimeException("Intentional failure");
            }
            this.properties = properties;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }

    public static class WithDefaultConstructor extends AbstractProcessor {
        public WithDefaultConstructor() {

        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }

}
