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
        if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
            return getUnicesPid(process, logger);
        } else if (process.getClass().getName().equals("java.lang.Win32Process")
                || process.getClass().getName().equals("java.lang.ProcessImpl")) {
            return getWindowsProcessId(process, logger);
        }

        return null;
    }

}
