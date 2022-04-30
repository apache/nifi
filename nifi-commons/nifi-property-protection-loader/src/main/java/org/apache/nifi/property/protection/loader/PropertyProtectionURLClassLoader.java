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
package org.apache.nifi.property.protection.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Property Protection URL Class Loader uses the Current Thread Class Loader as the parent and loads libraries from a standard directory
 */
public class PropertyProtectionURLClassLoader extends URLClassLoader {
    private static final String STANDARD_DIRECTORY = "lib/properties";

    private static final Logger logger = LoggerFactory.getLogger(PropertyProtectionURLClassLoader.class);

    public PropertyProtectionURLClassLoader(final ClassLoader parentClassLoader) {
        super(getPropertyProtectionUrls(), parentClassLoader);
    }

    private static URL[] getPropertyProtectionUrls() {
        final Path standardDirectory = Paths.get(STANDARD_DIRECTORY);
        if (Files.exists(standardDirectory)) {
            try (final Stream<Path> files = Files.list(standardDirectory)) {
                return files.map(Path::toUri)
                        .map(uri -> {
                            try {
                                return uri.toURL();
                            } catch (final MalformedURLException e) {
                                throw new UncheckedIOException(String.format("Processing Property Protection libraries failed [%s]", standardDirectory), e);
                            }
                        })
                        .toArray(URL[]::new);
            } catch (final IOException e) {
                throw new UncheckedIOException(String.format("Loading Property Protection libraries failed [%s]", standardDirectory), e);
            }
        } else {
            logger.warn("Property Protection libraries directory [{}] not found", standardDirectory);
            return new URL[0];
        }
    }
}
