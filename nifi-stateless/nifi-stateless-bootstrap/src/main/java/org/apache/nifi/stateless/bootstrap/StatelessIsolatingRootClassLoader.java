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
package org.apache.nifi.stateless.bootstrap;

import org.apache.nifi.nar.IsolatingRootClassLoader;

public class StatelessIsolatingRootClassLoader extends IsolatingRootClassLoader {

    public StatelessIsolatingRootClassLoader(ClassLoader parent) {
        super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // org.apache.nifi.registry needs to be passed through
        // because ExecuteStateless processor creates DataflowDefinition<VersionedFlowSnapshot> on its own
        // and the stateless engine must see the same VersionedFlowSnapshot class as ExecuteStateless
        if (name.startsWith("org.apache.nifi.registry")) {
            return loadClassNonIsolated(name, resolve);
        } else {
            return null;
        }
    }

}
