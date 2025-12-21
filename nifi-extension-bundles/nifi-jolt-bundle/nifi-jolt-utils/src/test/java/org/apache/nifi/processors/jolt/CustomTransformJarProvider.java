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
package org.apache.nifi.processors.jolt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public final class CustomTransformJarProvider {
    private static final String CLASS_ENTRY = "org/apache/nifi/processors/jolt/TestCustomJoltTransform.class";
    private static final String CUSTOM_TRANSFORM_CLASS_NAME = "org.apache.nifi.processors.jolt.TestCustomJoltTransform";

    private CustomTransformJarProvider() {
    }

    public static String getCustomTransformClassName() {
        return CUSTOM_TRANSFORM_CLASS_NAME;
    }

    public static Path createCustomTransformJar(Path directory) throws IOException {
        Files.createDirectories(directory);
        final Path jarPath = directory.resolve("TestCustomJoltTransform.jar");
        try (final InputStream classStream = CustomTransformJarProvider.class.getClassLoader().getResourceAsStream(CLASS_ENTRY)) {
            if (classStream == null) {
                throw new IllegalStateException("TestCustomJoltTransform class not found on classpath");
            }
            try (final JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarPath))) {
                final JarEntry jarEntry = new JarEntry(CLASS_ENTRY);
                jarOutputStream.putNextEntry(jarEntry);
                classStream.transferTo(jarOutputStream);
                jarOutputStream.closeEntry();
            }
        }
        return jarPath;
    }
}
