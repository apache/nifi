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
package org.apache.nifi.fileresource.service.api;

import java.io.InputStream;
import java.util.Objects;

/**
 * File Resource abstraction wraps an InputStream provides associated size in bytes
 */
public class FileResource {

    private final InputStream inputStream;

    private final long size;

    /**
     * File Resource constructor with required Input Stream and associated size in bytes
     *
     * @param inputStream Input Stream required
     * @param size Size of stream in bytes
     */
    public FileResource(final InputStream inputStream, final long size) {
        this.inputStream = Objects.requireNonNull(inputStream, "Input Stream required");
        this.size = size;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public long getSize() {
        return size;
    }
}
