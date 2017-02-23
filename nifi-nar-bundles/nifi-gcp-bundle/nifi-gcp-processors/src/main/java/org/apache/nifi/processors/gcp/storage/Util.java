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
package org.apache.nifi.processors.gcp.storage;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class(es) for Storage functionality.
 */
class Util {

     private static final Pattern CONTENT_DISPOSITION_PATTERN =
            Pattern.compile("^(.+);\\s*filename\\s*=\\s*\"([^\"]*)\"");
    /**
     * Parses the filename from a <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1">Content-Disposition</a>
     * header.
     * @param contentDisposition The Content-Disposition header to be parsed
     * @return the parsed content disposition.
     */
    public static ParsedContentDisposition parseContentDisposition(String contentDisposition) {
        Matcher m = CONTENT_DISPOSITION_PATTERN.matcher(contentDisposition);
        if (m.find() && m.groupCount() == 2) {
            return new ParsedContentDisposition(m.group(1), m.group(2));
        }
        return null;
    }

    public static class ParsedContentDisposition {
        private final String contentDispositionType;
        private final String fileName;

        private ParsedContentDisposition(String contentDispositionType, String fileName) {
            this.contentDispositionType = contentDispositionType;
            this.fileName = fileName;
        }


        public String getFileName() {
            return this.fileName;
        }

        public String getContentDispositionType() {
            return this.contentDispositionType;
        }
    }
}
