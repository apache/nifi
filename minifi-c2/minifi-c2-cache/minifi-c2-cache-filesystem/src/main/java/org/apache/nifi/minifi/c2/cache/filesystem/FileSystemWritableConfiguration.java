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

package org.apache.nifi.minifi.c2.cache.filesystem;

import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.util.DelegatingOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class FileSystemWritableConfiguration implements WriteableConfiguration {
    private final FileSystemConfigurationCache cache;
    private final Path path;
    private final String version;

    public FileSystemWritableConfiguration(FileSystemConfigurationCache cache, Path path, String version) {
        this.cache = cache;
        this.path = path;
        this.version = version;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public boolean exists() {
        return Files.exists(path);
    }

    @Override
    public OutputStream getOutputStream() throws ConfigurationProviderException {
        try {
            Path parent = path.getParent();
            Files.createDirectories(parent);
            Path tmpPath = cache.resolveChildAndVerifyParent(parent, path.getFileName().toString() + "." + UUID.randomUUID().toString());
            return new DelegatingOutputStream(Files.newOutputStream(tmpPath)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    Files.move(tmpPath, path);
                }
            };
        } catch (IOException e) {
            throw new ConfigurationProviderException("Unable to open " + path + " for writing.", e);
        }
    }

    @Override
    public InputStream getInputStream() throws ConfigurationProviderException {
        try {
            return Files.newInputStream(path, StandardOpenOption.READ);
        } catch (IOException e) {
            if (Files.exists(path)) {
                throw new ConfigurationProviderException("Unable to open " + path + " for reading.", e);
            } else {
                throw new InvalidParameterException("File not found: " + path, e);
            }
        }
    }

    @Override
    public String getName() {
        return path.getFileName().toString();
    }

    @Override
    public String toString() {
        return "FileSystemWritableConfiguration{path=" + path + ", version='" + version + "'}";
    }
}
