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
package org.apache.nifi.authorization;

import org.apache.commons.lang3.ClassUtils;

import java.lang.reflect.Proxy;
import java.util.List;

public final class AccessPolicyProviderFactory {

    public static AccessPolicyProvider withNarLoader(final AccessPolicyProvider baseAccessPolicyProvider, final ClassLoader classLoader) {
        final AccessPolicyProviderInvocationHandler invocationHandler = new AccessPolicyProviderInvocationHandler(baseAccessPolicyProvider, classLoader);

        // extract all interfaces... baseAccessPolicyProvider is non null so getAllInterfaces is non null
        final List<Class<?>> interfaceList = ClassUtils.getAllInterfaces(baseAccessPolicyProvider.getClass());
        final Class<?>[] interfaces = interfaceList.toArray(new Class<?>[interfaceList.size()]);

        return (AccessPolicyProvider) Proxy.newProxyInstance(classLoader, interfaces, invocationHandler);
    }

    private AccessPolicyProviderFactory() {}
}
