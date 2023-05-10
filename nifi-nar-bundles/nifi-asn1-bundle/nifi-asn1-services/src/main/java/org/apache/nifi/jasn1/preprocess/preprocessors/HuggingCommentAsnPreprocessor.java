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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HuggingCommentAsnPreprocessor implements AsnPreprocessor {
    public static final Pattern HUGGING_COMMENT_PATTERN = Pattern.compile("^(.*[^\\s])(--.*)$");

    @Override
    public List<String> preprocessAsn(List<String> lines) {
        final List<String> preprocessedLines = new ArrayList<>();

        lines.forEach(line -> {
            final StringBuilder preprocessedLine = new StringBuilder();

            final Matcher huggingCommentMather = HUGGING_COMMENT_PATTERN.matcher(line);
            if (huggingCommentMather.matches()) {
                preprocessedLine.append(huggingCommentMather.group(1))
                        .append(" ")
                        .append(huggingCommentMather.group(2));
                preprocessedLines.add(preprocessedLine.toString());
            } else {
                preprocessedLines.add(line);
            }
        });

        return preprocessedLines;
    }
}
