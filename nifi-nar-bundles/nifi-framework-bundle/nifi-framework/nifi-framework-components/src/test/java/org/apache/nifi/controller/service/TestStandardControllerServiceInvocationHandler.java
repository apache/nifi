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

package org.apache.nifi.controller.service;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestStandardControllerServiceInvocationHandler {

    private ClassLoader originalClassLoader;

    @Before
    public void setEmptyClassLoader() {
        this.originalClassLoader = Thread.currentThread().getContextClassLoader();

        // Change context class loader to a new, empty class loader so that calls to Controller Service will need to proxy returned objects.
        final URLClassLoader classLoader = new URLClassLoader(new URL[] {}, null);
        Thread.currentThread().setContextClassLoader(classLoader);
    }

    @After
    public void setOriginalClassLoaderBack() {
        if (originalClassLoader != null) Thread.currentThread().setContextClassLoader(originalClassLoader);
    }

    @Test
    public void testSimpleProxy() {
        final BaseControllerService proxiedService = createProxyService();
        assertEquals(1, proxiedService.getLevel());

        final Integer integer = proxiedService.getLevel();
        assertEquals(Integer.valueOf(1), integer);

        final String toStringValue = proxiedService.toString();
        assertFalse(Proxy.isProxyClass(toStringValue.getClass()));

        BaseControllerService nextLevel = proxiedService.getNextLevel();
        assertNotNull(nextLevel);
        assertEquals(2, nextLevel.getLevel());
        assertFalse(nextLevel instanceof TestService);
        assertTrue(Proxy.isProxyClass(nextLevel.getClass()));

        final BaseControllerService thirdLevel = nextLevel.getNextLevel();
        assertNotNull(thirdLevel);
        assertEquals(3, thirdLevel.getLevel());
        assertFalse(thirdLevel instanceof TestService);
        assertTrue(Proxy.isProxyClass(nextLevel.getClass()));

        for (int i=0; i < 5; i++) {
            assertEquals(i + 2, nextLevel.getLevel());
            assertFalse(nextLevel instanceof TestService);
            assertTrue(Proxy.isProxyClass(nextLevel.getClass()));
            nextLevel = nextLevel.getNextLevel();
        }
    }


    @Test
    public void testObjectsUsedWithinProxyNotProxied() {
        final BaseControllerService proxiedService = createProxyService();
        assertEquals(1, proxiedService.getLevel());

        proxiedService.assertNotProxied();
    }

    @Test
    public void testObjectsCreatedByServiceNotProxiedWhenHandedBack() {
        final BaseControllerService proxiedService = createProxyService();
        assertEquals(1, proxiedService.getLevel());

        final BaseControllerService nextLevel = proxiedService.getNextLevel();

        // The nextLevel that is returned should be proxied. This ensures that any method invoked on this
        // class is invoked with the appropriate thread context class loader in place.
        assertTrue(Proxy.isProxyClass(nextLevel.getClass()));

        // When the proxied object is handed back to the Controller Service, though, the service should receive a
        // version that is not proxied.
        proxiedService.assertNotProxied(nextLevel);
    }


    private BaseControllerService createProxyService() {
        final ExtensionManager extensionManager = Mockito.mock(ExtensionManager.class);
        final TestService testService = new TestService();
        testService.setLevel(1);

        final ControllerServiceNode serviceNode = Mockito.mock(ControllerServiceNode.class);
        Mockito.when(serviceNode.getState()).thenReturn(ControllerServiceState.ENABLED);

        final StandardControllerServiceInvocationHandler handler = new StandardControllerServiceInvocationHandler(extensionManager, testService);
        handler.setServiceNode(serviceNode);

        return (BaseControllerService) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {BaseControllerService.class}, handler);
    }


    public interface BaseControllerService extends ControllerService {
        BaseControllerService getNextLevel();
        int getLevel();
        void assertNotProxied();
        void assertNotProxied(BaseControllerService service);
    }

    public static class TestService extends AbstractControllerService implements BaseControllerService {
        private int level;

        @Override
        public BaseControllerService getNextLevel() {
            final TestService nextLevel = new TestService();
            nextLevel.setLevel(level + 1);
            return nextLevel;
        }

        @Override
        public int getLevel() {
            return level;
        }

        public void setLevel(final int level) {
            this.level = level;
        }

        @Override
        public void assertNotProxied() {
            BaseControllerService nextLevel = getNextLevel();
            for (int i=0; i < 5; i++) {
                assertEquals(level + i + 1, nextLevel.getLevel());
                assertTrue(nextLevel instanceof TestService);
                assertFalse(Proxy.isProxyClass(nextLevel.getClass()));

                nextLevel = nextLevel.getNextLevel();
            }
        }

        @Override
        public void assertNotProxied(final BaseControllerService service) {
            assertTrue(service instanceof TestService);
            assertFalse(Proxy.isProxyClass(service.getClass()));
        }
    }
}
