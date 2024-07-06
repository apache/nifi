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

package org.apache.nifi.processors.email;

import java.nio.charset.StandardCharsets;

public class GenerateAttachment {
    String from;
    String to;
    String subject;
    String message;

    private static final String NEWLINE = "\n";

    private static final String BOUNDARY = "5A7C0449-336B-4F73-81EF-F176E4DF44B2";

    public GenerateAttachment(String from, String to, String subject, String message, String hostName) {
        this.from = from;
        this.to = to;
        this.subject = subject;
        this.message = message;
    }

    public byte[] simpleMessage() {
        return simpleMessage(null);
    }

    public byte[] simpleMessage(final String recipient) {
        final StringBuilder builder = new StringBuilder();

        builder.append("MIME-Version: 1.0");
        builder.append(NEWLINE);
        builder.append("Content-Type: text/plain; charset=utf-8");
        builder.append(NEWLINE);
        builder.append("From: ");
        builder.append(from);
        builder.append(NEWLINE);

        if (recipient != null) {
            builder.append("To: ");
            builder.append(recipient);
            builder.append(NEWLINE);
        }

        builder.append("Subject: ");
        builder.append(subject);
        builder.append(NEWLINE);
        builder.append(NEWLINE);
        builder.append(message);

        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public byte[] withAttachments(int amount) {
        final StringBuilder builder = new StringBuilder();

        builder.append("MIME-Version: 1.0");
        builder.append(NEWLINE);

        builder.append("Content-Type: multipart/mixed; boundary=\"");
        builder.append(BOUNDARY);
        builder.append("\"");
        builder.append(NEWLINE);

        builder.append("From: ");
        builder.append(from);
        builder.append(NEWLINE);
        builder.append("To: ");
        builder.append(to);
        builder.append(NEWLINE);
        builder.append("Subject: ");
        builder.append(subject);
        builder.append(NEWLINE);
        builder.append(NEWLINE);

        for (int i = 0; i < amount; i++) {
            builder.append("--");
            builder.append(BOUNDARY);
            builder.append(NEWLINE);
            builder.append("Content-Type: text/plain; charset=utf-8");
            builder.append(NEWLINE);
            builder.append("Content-Disposition: attachment; filename=\"pom.xml-%d\"".formatted(i));
            builder.append(NEWLINE);
            builder.append(NEWLINE);
            builder.append("Attachment");
            builder.append(i);
            builder.append(NEWLINE);
        }

        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }
}
