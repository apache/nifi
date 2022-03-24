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

import com.sun.jna.platform.win32.Kernel32Util;
import org.junit.runners.model.InitializationError;

import java.util.HashMap;
import java.util.Map;

/**
 * Can't even use the JNA interface classes if the native library won't load.  This is a workaround to allow mocking them for unit tests.
 */
public class JNAJUnitRunner extends JNAOverridingJUnitRunner {
    public static final String TEST_COMPUTER_NAME = "testComputerName";
    public static final String KERNEL_32_UTIL_CANONICAL_NAME = Kernel32Util.class.getCanonicalName();

    public JNAJUnitRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected Map<String, Map<String, String>> getClassOverrideMap() {
        Map<String, Map<String, String>> classOverrideMap = new HashMap<>();

        Map<String, String> nativeOverrideMap = new HashMap<>();
        nativeOverrideMap.put(LOAD_LIBRARY, "return null;");
        classOverrideMap.put(NATIVE_CANONICAL_NAME, nativeOverrideMap);

        Map<String, String> kernel32UtilMap = new HashMap<>();
        kernel32UtilMap.put("getComputerName", "return \"" + TEST_COMPUTER_NAME + "\";");
        classOverrideMap.put(KERNEL_32_UTIL_CANONICAL_NAME, kernel32UtilMap);

        return classOverrideMap;
    }
}
