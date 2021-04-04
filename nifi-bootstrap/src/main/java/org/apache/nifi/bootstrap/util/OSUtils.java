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

package org.apache.nifi.bootstrap.util;

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;

/**
 * OS specific utilities with generic method interfaces
 */
public final class OSUtils {
    /**
     * @param process NiFi Process Reference
     * @param logger  Logger Reference for Debug
     * @return        Returns pid or null in-case pid could not be determined
     * This method takes {@link Process} and {@link Logger} and returns
     * the platform specific ProcessId for Unix like systems or Handle for Win32 Systems, a.k.a <b>pid</b>
     * In-case it fails to determine the pid, it will return Null.
     * Purpose for the Logger is to log any interaction for debugging.
     */
    public static Long getProcessId(final Process process, final Logger logger) {
        /*
         * NIFI-5175: NiFi built with Java 1.8 and running on Java 9.  Reflectively invoke Process.pid() on
         * the given process instance to get the PID of this Java process.  Reflection is required in this scenario
         * due to NiFi being compiled on Java 1.8, which does not have the Process API improvements available in
         * Java 9.
         *
         * Otherwise, if NiFi is running on Java 1.8, attempt to get PID using capabilities available on Java 1.8.
         *
         * TODO: When minimum Java version updated to Java 9+, this class should be removed with the addition
         * of the pid method to the Process API.
         */
        Long pid = null;
        try {
            // Get Process.pid() interface method to avoid illegal reflective access
            final Method pidMethod = Process.class.getDeclaredMethod("pid");
            final Object pidNumber = pidMethod.invoke(process);
            if (pidNumber instanceof Long) {
                pid = (Long) pidNumber;
            }
        } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            final String processClassName = process.getClass().getName();
            if (processClassName.equals("java.lang.UNIXProcess")) {
                pid = getUnixPid(process, logger);
            } else if (processClassName.equals("java.lang.Win32Process")
                    || processClassName.equals("java.lang.ProcessImpl")) {
                pid = getWindowsProcessId(process, logger);
            } else {
                logger.debug("Failed to determine Process ID from [{}]: {}", processClassName, e.getMessage());
            }
        }

        return pid;
    }

    /**
     * Returns the major version parsed from the provided Java version string (e.g. {@code "1.8.0.231"} -> {@code 8}).
     *
     * @param version the Java version string
     * @return the major version as an int
     */
    public static int parseJavaVersion(final String version) {
        String majorVersion;
        if (version.startsWith("1.")) {
            majorVersion = version.substring(2, 3);
        } else {
            Pattern majorVersion9PlusPattern = Pattern.compile("(\\d+).*");
            Matcher m = majorVersion9PlusPattern.matcher(version);
            if (m.find()) {
                majorVersion = m.group(1);
            } else {
                throw new IllegalArgumentException("Could not detect major version of " + version);
            }
        }
        return Integer.parseInt(majorVersion);
    }

    /**
     * @param process NiFi Process Reference
     * @param logger  Logger Reference for Debug
     * @return        Returns pid or null in-case pid could not be determined
     * This method takes {@link Process} and {@link Logger} and returns
     * the platform specific ProcessId for Unix like systems, a.k.a <b>pid</b>
     * In-case it fails to determine the pid, it will return Null.
     * Purpose for the Logger is to log any interaction for debugging.
     */
    private static Long getUnixPid(final Process process, final Logger logger) {
        try {
            final Class<?> procClass = process.getClass();
            final Field pidField = procClass.getDeclaredField("pid");
            pidField.setAccessible(true);
            final Object pidObject = pidField.get(process);

            if (pidObject instanceof Number) {
                return ((Number) pidObject).longValue();
            }
            return null;
        } catch (final IllegalAccessException | NoSuchFieldException e) {
            logger.debug("Could not find Unix PID", e);
            return null;
        }
    }

    /**
     * @param process NiFi Process Reference
     * @param logger  Logger Reference for Debug
     * @return        Returns pid or null in-case pid could not be determined
     * This method takes {@link Process} and {@link Logger} and returns
     * the platform specific Handle for Win32 Systems, a.k.a <b>pid</b>
     * In-case it fails to determine the pid, it will return Null.
     * Purpose for the Logger is to log any interaction for debugging.
     */
    private static Long getWindowsProcessId(final Process process, final Logger logger) {
        Long pid = null;
        try {
            final Field handleField = process.getClass().getDeclaredField("handle");
            handleField.setAccessible(true);
            long peer = handleField.getLong(process);

            final Kernel32 kernel = Kernel32.INSTANCE;
            final WinNT.HANDLE handle = new WinNT.HANDLE();
            handle.setPointer(Pointer.createConstant(peer));
            pid = Long.valueOf(kernel.GetProcessId(handle));
        } catch (final IllegalAccessException | NoSuchFieldException e) {
            logger.debug("Could not find Windows PID", e);
        }
        return pid;
    }
}
