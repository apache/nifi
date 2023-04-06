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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConstraintAsnPreprocessor implements AsnPreprocessor {
    public static final String OPEN_BRACKET = "(";
    public static final String CLOSE_BRACKET = ")";

    public static final Pattern ALLOWED = Pattern.compile("^(\\d+\\))(.*)");

    @Override
    public List<String> preprocessAsn(List<String> lines) {
        final List<String> preprocessedLines = new ArrayList<>();

        final AtomicInteger unclosedCounter = new AtomicInteger(0);
        lines.forEach(line -> {
            final StringBuilder preprocessedLine = new StringBuilder();

            String contentToProcess = line;

            while (contentToProcess.contains(OPEN_BRACKET) || contentToProcess.contains(CLOSE_BRACKET)) {
                if (contentToProcess.matches("^\\s*--.*$")) {
                    break;
                }

                final int openBracketIndex = contentToProcess.indexOf(OPEN_BRACKET);
                final int closeBracketIndex = contentToProcess.indexOf(CLOSE_BRACKET);

                if (openBracketIndex != -1 && (openBracketIndex < closeBracketIndex) || closeBracketIndex == -1) {
                    final String contentBeforeOpenBracket = contentToProcess.substring(0, openBracketIndex);
                    final String contentAfterOpenBracket = contentToProcess.substring(openBracketIndex + 1);

                    if (unclosedCounter.get() < 1) {
                        if (!contentBeforeOpenBracket.isEmpty()) {
                            preprocessedLine.append(contentBeforeOpenBracket + " ");
                            // Adding a space " " because (...) blocks can serve as separators so removing them might
                            //  join together parts that should stay separated
                        }

                        final Matcher supportedMatcher = ALLOWED.matcher(contentAfterOpenBracket);
                        if (supportedMatcher.matches()) {
                            preprocessedLine.append(OPEN_BRACKET + supportedMatcher.group(1));
                            contentToProcess = supportedMatcher.group(2);
                            continue;
                        }
                    }

                    unclosedCounter.incrementAndGet();

                    contentToProcess = contentAfterOpenBracket;
                } else if (closeBracketIndex != -1) {
                    unclosedCounter.decrementAndGet();

                    contentToProcess = contentToProcess.substring(closeBracketIndex + 1);
                }
            }

            if (unclosedCounter.get() < 1) {
                if (!contentToProcess.isEmpty()) {
                    preprocessedLine.append(contentToProcess);
                }
            }

            preprocessedLines.add(preprocessedLine.toString());
        });

        return preprocessedLines;
    }
}
