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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;

/**
 * OS specific utilities with generic method interfaces
 */
public final class OSUtils {
    /**
     * @param process NiFi Process Reference
     * @param logger  Logger Reference for Debug
     * @return        Returns pid or null in-case pid could not be determined
     * This method takes {@link Process} and {@link Logger} and returns
     * the platform specific ProcessId for Unix like systems, a.k.a <b>pid</b>
     * In-case it fails to determine the pid, it will return Null.
     * Purpose for the Logger is to log any interaction for debugging.
     */
    private static Long getUnicesPid(final Process process, final Logger logger) {
        try {
            final Class<?> procClass = process.getClass();
            final Field pidField = procClass.getDeclaredField("pid");
            pidField.setAccessible(true);
            final Object pidObject = pidField.get(process);

            logger.debug("PID Object = {}", pidObject);

            if (pidObject instanceof Number) {
                return ((Number) pidObject).longValue();
            }
            return null;
        } catch (final IllegalAccessException | NoSuchFieldException nsfe) {
            logger.debug("Could not find PID for child process due to {}", nsfe);
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
        /* determine the pid on windows plattforms */
        try {
            Field f = process.getClass().getDeclaredField("handle");
            f.setAccessible(true);
            long handl = f.getLong(process);

            Kernel32 kernel = Kernel32.INSTANCE;
            WinNT.HANDLE handle = new WinNT.HANDLE();
            handle.setPointer(Pointer.createConstant(handl));
            int ret = kernel.GetProcessId(handle);
            logger.debug("Detected pid: {}", ret);
            return Long.valueOf(ret);
        } catch (final IllegalAccessException | NoSuchFieldException nsfe) {
            logger.debug("Could not find PID for child process due to {}", nsfe);
        }
        return null;
    }

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
        if (!System.getProperty("java.version").startsWith("1.")) {
            try {
                Method pidMethod = process.getClass().getMethod("pid");
                pidMethod.setAccessible(true);
                Object pidMethodResult = pidMethod.invoke(process);
                if (Long.class.isAssignableFrom(pidMethodResult.getClass())) {
                    pid = (Long) pidMethodResult;
                } else {
                    logger.debug("Could not determine PID for child process because returned PID was not " +
                            "assignable to type " + Long.class.getName());
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                logger.debug("Could not find PID for child process due to {}", e);
            }
        } else if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
            pid = getUnicesPid(process, logger);
        } else if (process.getClass().getName().equals("java.lang.Win32Process")
                || process.getClass().getName().equals("java.lang.ProcessImpl")) {
            pid = getWindowsProcessId(process, logger);
        }

        return pid;
    }

}
