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
package org.apache.nifi.processors.splunk.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class LogGenerator {

    static final String LOG_MESSAGE = "This is log message # %s";

    private final int numLogs;
    private final String delimiter;

    public LogGenerator(int numLogs, String delimiter) {
        this.numLogs = numLogs;
        this.delimiter = delimiter;
    }

    public void generate(final File file) throws IOException {
        try (OutputStream rawOut = new FileOutputStream(file);
             BufferedOutputStream out = new BufferedOutputStream(rawOut)) {

            for (int i = 0; i < numLogs; i++) {
                if (i > 0) {
                    out.write(delimiter.getBytes(StandardCharsets.UTF_8));
                }

                final String message = String.format(LOG_MESSAGE, i);
                out.write(message.getBytes(StandardCharsets.UTF_8));
            }

            System.out.println("Done generating " + numLogs + " messages");
            out.flush();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 3) {
            System.err.println("USAGE: LogGenerator <num_logs> <delimiter> <file>");
            System.exit(1);
        }

        final int numLogs = Integer.parseInt(args[0]);
        final String delim = args[1];

        final File file = new File(args[2]);
        if (file.exists()) {
            file.delete();
        }

        final LogGenerator generator = new LogGenerator(numLogs, delim);
        generator.generate(file);
    }
}
