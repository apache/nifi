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

package org.apache.nifi.minifi.util;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util for creating class loader with bootstrap libs.
 */
public final class BootstrapClassLoaderUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapClassLoaderUtils.class);
    private static final String LIB_BOOTSTRAP_DIR = "lib/bootstrap";

    private BootstrapClassLoaderUtils() {

    }

    public static ClassLoader createBootstrapClassLoader() {
        List<URL> urls = new ArrayList<>();
        try (Stream<Path> files = Files.list(Paths.get(LIB_BOOTSTRAP_DIR))) {
            files.forEach(p -> {
                try {
                    urls.add(p.toUri().toURL());
                } catch (MalformedURLException mef) {
                    LOGGER.warn("Unable to load bootstrap library [{}]", p.getFileName(), mef);
                }
            });
        } catch (IOException ioe) {
            LOGGER.warn("Unable to access lib/bootstrap to create bootstrap classloader", ioe);
        }
        return new URLClassLoader(urls.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
    }
}
