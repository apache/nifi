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
package org.apache.nifi.processors.standard.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArgumentUtils {
    private final static char QUOTE = '"';
    private final static List<Character> DELIMITING_CHARACTERS = new ArrayList<>(3);

    static {
        DELIMITING_CHARACTERS.add('\t');
        DELIMITING_CHARACTERS.add('\r');
        DELIMITING_CHARACTERS.add('\n');
    }

    public static List<String> splitArgs(final String input, final char definedDelimiter) {
        if (input == null) {
            return Collections.emptyList();
        }

        final List<String> args = new ArrayList<>();

        final String trimmed = input.trim();
        boolean inQuotes = false;
        final StringBuilder sb = new StringBuilder();

        for (int i = 0; i < trimmed.length(); i++) {
            final char c = trimmed.charAt(i);

            if (DELIMITING_CHARACTERS.contains(c) || c == definedDelimiter) {
                if (inQuotes) {
                    sb.append(c);
                } else {
                    final String arg = sb.toString().trim();
                    if (!arg.isEmpty()) {
                        args.add(arg);
                    }
                    sb.setLength(0);
                }
                continue;
            }

            if (c == QUOTE) {
                inQuotes = !inQuotes;
                continue;
            }

            sb.append(c);
        }

        final String finalArg = sb.toString().trim();

        if (!finalArg.isEmpty()) {
            args.add(finalArg);
        }

        return args;
    }
}
