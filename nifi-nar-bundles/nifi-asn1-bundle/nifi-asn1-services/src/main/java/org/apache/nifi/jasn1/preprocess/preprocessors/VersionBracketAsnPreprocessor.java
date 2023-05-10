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
package org.apache.nifi.jasn1.preprocess.preprocessors;

import org.apache.nifi.jasn1.preprocess.AsnPreprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class VersionBracketAsnPreprocessor implements AsnPreprocessor {
    public static final String OPEN_VERSION_BRACKET = "[[";
    public static final String CLOSE_VERSION_BRACKET = "]]";

    public static final Pattern ONLY_WHITESPACES = Pattern.compile("^\\s*$");
    public static final Pattern ONLY_COMMENT = Pattern.compile("^\\s*--.*$");
    public static final Pattern IS_OPTIONAL_ALREADY = Pattern.compile(".*OPTIONAL\\s*,?\\s*$");

    public static final String TRAILING_COMMA_WITH_POTENTIAL_WHITESPACES = "(\\s*)(,?)(\\s*)$";
    public static final String ADD_OPTIONAL = " OPTIONAL$1$2$3";

    @Override
    public List<String> preprocessAsn(List<String> lines) {
        final List<String> preprocessedLines = new ArrayList<>();

        final AtomicBoolean inVersionBracket = new AtomicBoolean(false);
        lines.forEach(line -> {
            final StringBuilder preprocessedLine = new StringBuilder();
            String contentToProcess = line;

            final int versionBracketStart = contentToProcess.indexOf(OPEN_VERSION_BRACKET);
            if (versionBracketStart > -1) {
                inVersionBracket.set(true);

                final String contentBeforeVersionBracket = line.substring(0, versionBracketStart);
                if (!contentBeforeVersionBracket.isEmpty()) {
                    preprocessedLine.append(contentBeforeVersionBracket);
                }

                contentToProcess = contentToProcess.substring(versionBracketStart + 2);
            }

            final int versionBracketEnd = contentToProcess.indexOf(CLOSE_VERSION_BRACKET);
            String contentAfterVersionBracket = null;
            if (versionBracketEnd > -1) {
                contentAfterVersionBracket = contentToProcess.substring(versionBracketEnd + 2);
                contentToProcess = contentToProcess.substring(0, versionBracketEnd);
            }

            if (inVersionBracket.get()
                    && !ONLY_WHITESPACES.matcher(contentToProcess).matches()
                    && !ONLY_COMMENT.matcher(contentToProcess).matches()
                    && !IS_OPTIONAL_ALREADY.matcher(contentToProcess).matches()
            ) {
                contentToProcess = contentToProcess.replaceFirst(TRAILING_COMMA_WITH_POTENTIAL_WHITESPACES, ADD_OPTIONAL);
            }

            if (!contentToProcess.isEmpty()) {
                preprocessedLine.append(contentToProcess);
            }

            if (contentAfterVersionBracket != null) {
                if (!contentAfterVersionBracket.isEmpty()) {
                    preprocessedLine.append(contentAfterVersionBracket);
                }
                inVersionBracket.set(false);
            }

            preprocessedLines.add(preprocessedLine.toString());
        });

        return preprocessedLines;
    }
}
