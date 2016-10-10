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

package org.apache.nifi.processors.windows.event.log.jna;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinError;

import java.lang.reflect.Modifier;
import java.util.Arrays;

/**
 * Reverse lookup of error number to the named error if possible
 */
public class ErrorLookup {
    private final Kernel32 kernel32;

    public ErrorLookup(Kernel32 kernel32) {
        this.kernel32 = kernel32;
    }

    public String getLastError() {
        int lastError = kernel32.GetLastError();
        return Arrays.stream(WinError.class.getDeclaredFields()).filter(field -> {
            try {
                return Modifier.isStatic(field.getModifiers())
                        && field.getType() == int.class
                        && field.getName().startsWith("ERROR")
                        && (int) field.get(null) == lastError;
            } catch (IllegalAccessException e) {
                return false;
            }
        }).map(field -> field.getName() + "(" + lastError + ")")
                .findFirst()
                .orElse(Integer.toString(lastError));
    }
}
