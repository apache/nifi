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

import org.apache.nifi.minifi.c2.api.InvalidParameterException;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCache;
import org.apache.nifi.minifi.c2.api.cache.ConfigurationCacheFileInfo;
import org.apache.nifi.minifi.c2.api.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FileSystemConfigurationCache implements ConfigurationCache {
    private final Path pathRoot;
    private final String pathPattern;

    public FileSystemConfigurationCache(String pathRoot, String pathPattern) throws IOException {
        this.pathRoot = Paths.get(System.getenv("C2_SERVER_HOME")).resolve(pathRoot).toAbsolutePath();
        Files.createDirectories(this.pathRoot);
        this.pathPattern = pathPattern;
    }

    protected Path resolveChildAndVerifyParent(Path parent, String s) throws InvalidParameterException {
        Path child = parent.resolve(s).toAbsolutePath();
        if (child.toAbsolutePath().getParent().equals(parent)) {
            return child;
        } else {
            throw new InvalidParameterException("Path entry " + s + " not child of " + parent);
        }
    }

    @Override
    public ConfigurationCacheFileInfo getCacheFileInfo(Map<String, List<String>> parameters) throws InvalidParameterException {
        String pathString = pathPattern;
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            if (entry.getValue().size() != 1) {
                throw new InvalidParameterException("Multiple values for same parameter not supported in this provider.");
            }
            pathString = pathString.replaceAll(Pattern.quote("${" + entry.getKey() + "}"), entry.getValue().get(0));
        }
        String[] split = pathString.split("/");
        for (String s1 : split) {
            int openBrace = s1.indexOf("${");
            if (openBrace >= 0 && openBrace < s1.length() + 2) {
                int closeBrace = s1.indexOf("}", openBrace + 2);
                if (closeBrace >= 0) {
                    throw new InvalidParameterException("Found unsubstituted variable " + s1.substring(openBrace + 2, closeBrace));
                }
            }
        }
        String[] splitPath = split;
        Path path = pathRoot.toAbsolutePath();
        for (int i = 0; i < splitPath.length - 1; i++) {
            String s = splitPath[i];
            path = resolveChildAndVerifyParent(path, s);
        }
        Pair<Path, String> dirPathAndFilename = new Pair<>(path, splitPath[splitPath.length - 1]);
        return new FileSystemCacheFileInfoImpl(this, dirPathAndFilename.getFirst(), dirPathAndFilename.getSecond() + ".v");
    }
}
