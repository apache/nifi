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
package org.apache.nifi.cef;

import com.fluenda.parcefone.event.CommonEvent;
import com.fluenda.parcefone.parser.CEFParser;
import org.apache.nifi.schema.inference.RecordSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Locale;

final class CEFRecordSource implements RecordSource<CommonEvent> {
    private final CEFParser parser;
    private final BufferedReader reader;
    private final Locale locale;
    private final boolean acceptEmptyExtensions;
    private final boolean failFast;

    CEFRecordSource(final InputStream in, final CEFParser parser, final Locale locale, final boolean acceptEmptyExtensions, final boolean failFast) {
        this.parser = parser;
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.locale = locale;
        this.acceptEmptyExtensions = acceptEmptyExtensions;
        this.failFast = failFast;
    }

    @Override
    public CommonEvent next() throws IOException {
        final String line = nextLine();

        if (line == null) {
            return null;
        }

        final CommonEvent event = parser.parse(line, false, acceptEmptyExtensions, locale);

        if (event == null && failFast) {
            throw new IOException("Could not parse event");
        }

        return event;
    }

    private String nextLine() throws IOException {
        String line;

        while((line = reader.readLine()) != null) {
            if (!line.isEmpty()) {
                break;
            }
        }
        return line;
    }
}
