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

package org.apache.nifi.controller;

/**
 * An interface that can be added to a Proxy object for a Controller Service in order to get the underlying object that is being proxied.
 * This is done so that any object that is returned by a Controller Service
 * can be unwrapped if it's passed back to the Controller Service. This is needed in order to accommodate for the following scenario.
 * Consider a Controller Service that has two methods:
 *
 * <pre><code>
 * public MyObject createObject();
 * public void useObject(MyObject object);
 * </code></pre>
 *
 * And further consider that MyObject is an interface with multiple implementations.
 * If the {@code useObject} method is implemented using logic such as:
 *
 * <pre><code>
 * public void useObject(final MyObject object) {
 *       if (object instanceof SomeObject) {
 *           // perform some action
 *       }
 * }
 * </code></pre>
 *
 * In this case, if the {@code createObject} method does in fact create an instance of {@code SomeObject}, the proxied object that is returned will not be of type {@code SomeObject}
 * because {@code SomeObject} is a class, not an interface. So the proxy implements the {@code MyObject} interface, but it is not an instance of {@code SomeObject}.
 * As a result, the instanceof check would return {@code false} but the service implementor should reasonably expect it to return {@code true}.
 * In order to accommodate this behavior, this interface can be added to the proxy and then the underlying object can be "unwrapped" when being provided to the Controller Service.
 *
 * The {@link java.lang.reflect.InvocationHandler InvocationHandler} is then able to implement the method in order to unwrap the object.
 *
 * @param <T> the type of the wrapped object
 */
public interface ControllerServiceProxyWrapper<T> {
    /**
     * @return the object that is being wrapped/proxied.
     */
    T getWrapped();
}
