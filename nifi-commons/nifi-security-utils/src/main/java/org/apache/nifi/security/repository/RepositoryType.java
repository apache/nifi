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
package org.apache.nifi.security.repository;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum RepositoryType {
    CONTENT("Content repository", "content.repository", "stream"),
    PROVENANCE("Provenance repository", "provenance.repository", "block"),
    FLOWFILE("Flowfile repository", "flowfile.repository", "stream");

    private static final Logger logger = LoggerFactory.getLogger(RepositoryType.class);

    private final String name;
    private final String packagePath;
    private final String encryptionProcess;

    RepositoryType(String name, String packagePath, String encryptionProcess) {
        this.name = name;
        this.packagePath = packagePath;
        this.encryptionProcess = encryptionProcess;
    }

    public String getName() {
        return name;
    }

    public String getPackagePath() {
        return packagePath;
    }

    public String getEncryptionProcess() {
        return encryptionProcess;
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("Repository", name);
        builder.append("Package path", packagePath);
        builder.append("Encryption process", encryptionProcess);
        return builder.toString();
    }

    /**
     * Uses loose string matching to determine the repository type from input. Matches to
     * content, provenance, or flowfile repository, or throws an {@link IllegalArgumentException}
     * on input which cannot be reasonably matched to any known repository type.
     *
     * @param input a string containing some description of the repository type
     * @return a matching instance of the RT enum
     */
    public static RepositoryType determineType(String input) {
        if (StringUtils.isBlank(input)) {
            throw new IllegalArgumentException("The input cannot be null or empty");
        }
        String lowercaseInput = input.toLowerCase().trim();

        // Use loose matching to handle prov[enance] and flow[ ][file]
        if (lowercaseInput.contains("content")) {
            return RepositoryType.CONTENT;
        } else if (lowercaseInput.contains("prov")) {
            return RepositoryType.PROVENANCE;
        } else if (lowercaseInput.contains("flow")) {
            return RepositoryType.FLOWFILE;
        } else {
            final String msg = "Could not determine repository type from '" + input + "'";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }
}
