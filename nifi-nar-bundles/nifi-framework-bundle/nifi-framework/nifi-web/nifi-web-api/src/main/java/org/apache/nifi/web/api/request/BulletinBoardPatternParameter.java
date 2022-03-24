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
package org.apache.nifi.web.api.request;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Parameter class that auto [ap|pre]pends '.*' to the specified pattern to make user input more user friendly.
 */
public class BulletinBoardPatternParameter {

    private static final String INVALID_PATTERN_MESSAGE = "Unable to parse pattern '%s'";

    private Pattern patternValue;

    public BulletinBoardPatternParameter(String rawPatternValue) {
        try {
            patternValue = Pattern.compile(String.format(".*%s.*", rawPatternValue));
        } catch (PatternSyntaxException pse) {
            throw new IllegalArgumentException(String.format(INVALID_PATTERN_MESSAGE, rawPatternValue));
        }
    }

    public Pattern getPattern() {
        return patternValue;
    }

    public String getRawPattern() {
        return patternValue.pattern();
    }
}
