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
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.cache.WriteableConfiguration;
import org.apache.nifi.minifi.c2.api.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

public class FileSystemCacheFileInfoImpl implements ConfigurationCacheFileInfo {
    private final FileSystemConfigurationCache cache;
    private final Path dirPath;
    private final String expectedFilename;
    private final int expectedFilenameLength;

    public FileSystemCacheFileInfoImpl(FileSystemConfigurationCache cache, Path dirPath, String expectedFilename) {
        this.cache = cache;
        this.dirPath = dirPath;
        this.expectedFilename = expectedFilename;
        this.expectedFilenameLength = expectedFilename.length();
    }

    @Override
    public Integer getVersionIfMatch(String filename) {
        if (!filename.startsWith(expectedFilename) || filename.length() == expectedFilenameLength) {
            return null;
        }
        try {
            return Integer.parseInt(filename.substring(expectedFilenameLength));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public Stream<WriteableConfiguration> getCachedConfigurations() throws IOException {
        return Files.list(dirPath).map(p -> {
            Integer version = getVersionIfMatch(p.getFileName().toString());
            if (version == null) {
                return null;
            }
            return new Pair<>(version, p);
        }).filter(Objects::nonNull)
                .sorted(Comparator.comparing(pair -> ((Pair<Integer, Path>) pair).getFirst())
                        .reversed()).map(pair -> new FileSystemWritableConfiguration(cache, pair.getSecond(), Integer.toString(pair.getFirst())));
    }

    @Override
    public WriteableConfiguration getConfiguration(Integer version) throws ConfigurationProviderException {
        if (version == null) {
            try {
                return getCachedConfigurations().findFirst().orElseThrow(() -> new ConfigurationProviderException("No configurations found for " + dirPath + "/" + expectedFilename + "[0-9]+"));
            } catch (IOException e) {
                throw new ConfigurationProviderException("Unable to get cached configurations.", e);
            }
        }
        try {
            return new FileSystemWritableConfiguration(cache, cache.resolveChildAndVerifyParent(dirPath, expectedFilename + version), Integer.toString(version));
        } catch (NumberFormatException e) {
            throw new InvalidParameterException("Expected numeric version.", e);
        }
    }
}
