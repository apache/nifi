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

package org.apache.nifi.processors.windows.event.log;

import org.junit.runners.model.InitializationError;

import java.util.HashMap;
import java.util.Map;

/**
 * Native load failure to simulate case on all OSes (even Windows)
 */
public class JNAFailJUnitRunner extends JNAOverridingJUnitRunner {

    public JNAFailJUnitRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected Map<String, Map<String, String>> getClassOverrideMap() {
        Map<String, Map<String, String>> classOverrideMap = new HashMap<>();
        Map<String, String> nativeOverrideMap = new HashMap<>();
        nativeOverrideMap.put(LOAD_LIBRARY, "throw new " + UnsatisfiedLinkError.class.getCanonicalName() + "(\"JNAFailJUnitRunner\");");
        classOverrideMap.put(NATIVE_CANONICAL_NAME, nativeOverrideMap);
        return classOverrideMap;
    }
}
