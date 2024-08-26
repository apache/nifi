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
package org.apache.nifi.bootstrap.command.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class FileResponseStreamHandlerTest {

    @Test
    void testOnResponseStream(@TempDir final Path outputDirectory) throws IOException {
        final Path outputPath = outputDirectory.resolve(FileResponseStreamHandlerTest.class.getSimpleName());

        final FileResponseStreamHandler handler = new FileResponseStreamHandler(outputPath);

        final byte[] bytes = String.class.getName().getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(bytes);

        handler.onResponseStream(inputStream);

        final byte[] read = Files.readAllBytes(outputPath);
        assertArrayEquals(bytes, read);
    }
}
