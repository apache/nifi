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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * File implementation responsible for reading and transferring responses
 */
public class FileResponseStreamHandler implements ResponseStreamHandler {
    private static final Logger logger = LoggerFactory.getLogger(FileResponseStreamHandler.class);

    private final Path outputPath;

    public FileResponseStreamHandler(final Path outputPath) {
        this.outputPath = Objects.requireNonNull(outputPath);
    }

    @Override
    public void onResponseStream(final InputStream responseStream) {
        try (OutputStream outputStream = Files.newOutputStream(outputPath)) {
            responseStream.transferTo(outputStream);
        } catch (final IOException e) {
            logger.warn("Write response stream failed for [%s]".formatted(outputPath), e);
        }
    }
}
