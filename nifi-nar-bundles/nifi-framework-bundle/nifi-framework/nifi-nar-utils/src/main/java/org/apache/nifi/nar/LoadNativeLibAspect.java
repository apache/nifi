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
package org.apache.nifi.nar;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * AspectJ aspect to handle native library loading by absolute path in multiple classloaders.
 *
 * The aspect intercepts the {@link System#load(String)} / {@link Runtime#load(String)} calls and creates a copy of the native library
 * in the system temp folder with a unique name, then passes this new path to the original load() method.
 * In this way, different classloaders will load different native libraries and the "Native Library ... already loaded in another classloader"
 * error can be avoided.
 *
 * To put it into effect, the AspectJ agent needs to be configured in bootstrap.conf (see the necessary config there, commented out by default).
 *
 * This aspect handles the native library loading when the library is being loaded by its absolute path.
 * For loading a native library by its logical name, see {@link AbstractNativeLibHandlingClassLoader}.
 */
@Aspect
public class LoadNativeLibAspect {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Around("call(void java.lang.System.load(String)) || call(void java.lang.Runtime.load(String))")
    public void around(ProceedingJoinPoint joinPoint) throws Throwable {
        String origLibPathStr = (String) joinPoint.getArgs()[0];

        if (origLibPathStr == null || origLibPathStr.isEmpty()) {
            logger.info("Native library path specified as null or empty string, proceeding normally");
            joinPoint.proceed();
            return;
        }

        Path origLibPath = Paths.get(origLibPathStr);

        if (!Files.exists(origLibPath)) {
            logger.info("Native library does not exist, proceeding normally");
            joinPoint.proceed();
            return;
        }

        String libFileName = origLibPath.getFileName().toString();

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        String prefix = contextClassLoader.getClass().getName() + "@" + contextClassLoader.hashCode() + "_";
        String suffix = "_" + libFileName;

        Path tempLibPath = Files.createTempFile(prefix, suffix);
        Files.copy(origLibPath, tempLibPath, REPLACE_EXISTING);

        logger.info("Loading native library via absolute path (original lib: {}, copied lib: {}", origLibPath, tempLibPath);
        joinPoint.proceed(new Object[]{tempLibPath.toString()});
    }
}
