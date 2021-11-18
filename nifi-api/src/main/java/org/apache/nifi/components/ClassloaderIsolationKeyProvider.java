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

package org.apache.nifi.components;

import org.apache.nifi.context.PropertyContext;

/**
 * <p>
 * There are times when a component must be created in such a way that each instance gets its own ClassLoader hierarchy,
 * rather than sharing the ClassLoader with other components (see {@link org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading @RequiresInstanceClassLoading}).
 * This, however, can be extremely expensive, as all of the classes must be loaded again for each instance of the component. When thousands of these
 * components are used in a single flow, the startup time can be great, and it can lead to massive amounts of RAM being required.
 * </p>
 *
 * <p>
 * For components that do require instance ClassLoading that clones ancestor resources, this interface can be optional implemented by the component.
 * If the interface is implemented, the component is given the opportunity to return a distinct "key" that can be used to identify instances that may share
 * the same ClassLoader.
 * </p>
 */
public interface ClassloaderIsolationKeyProvider {

    /**
     * <p>
     * Determines the key that identifies a shared ClassLoader that this component may use. Any two instances of the same component that return
     * the same key may be assigned the same base ClassLoader (though it is not guaranteed that this will be the case).
     * </p>
     *
     * <p>
     * If a subsequent call to this method returns a different value, the component will be recreated with a different ClassLoader.
     * </p>
     *
     * <p>
     * Implementation Note: for components that implement this interface, this method will be called often. Therefore, performance characteristics
     * of the implementation are critical. The method is expected to return the value of a configured property or derive a value to return based off
     * of the values of a few properties. Accessing a remote resource, is too expensive. If the necessary computation is non-trivial, then it should be
     * performed out of band and the pre-computed value simply returned by this method.
     * </p>
     *
     * @param context the PropertyContext that can be used for determining the key
     * @return a distinct key that can be used to indicate which shared ClassLoader is allowed to be used
     */
    String getClassloaderIsolationKey(PropertyContext context);

}
