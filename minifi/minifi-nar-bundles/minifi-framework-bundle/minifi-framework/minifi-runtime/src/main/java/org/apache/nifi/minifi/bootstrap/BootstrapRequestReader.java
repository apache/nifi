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

package org.apache.nifi.minifi.bootstrap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class BootstrapRequestReader {
    private final String secretKey;

    public BootstrapRequestReader(String secretKey) {
        this.secretKey = secretKey;
    }

    // we don't want to close the stream, as the caller will do that
    public BootstrapRequest readRequest(InputStream in) throws IOException {
        // We want to ensure that we don't try to read data from an InputStream directly
        // by a BufferedReader because any user on the system could open a socket and send
        // a multi-gigabyte file without any new lines in order to crash the MiNiFi instance
        // (or at least cause OutOfMemoryErrors, which can wreak havoc on the running instance).
        // So we will limit the Input Stream to only 4 KB, which should be plenty for any request.
        LimitingInputStream limitingIn = new LimitingInputStream(in, 4096);
        BufferedReader reader = new BufferedReader(new InputStreamReader(limitingIn));

        String line = reader.readLine();
        String[] splits = line.split(" ");
        if (splits.length < 1) {
            throw new IOException("Received invalid request from Bootstrap: " + line);
        }

        String requestType = splits[0];
        String[] args;
        if (splits.length == 1) {
            throw new IOException("Received invalid request from Bootstrap; request did not have a secret key; request type = " + requestType);
        } else if (splits.length == 2) {
            args = new String[0];
        } else {
            args = Arrays.copyOfRange(splits, 2, splits.length);
        }

        String requestKey = splits[1];
        if (!secretKey.equals(requestKey)) {
            throw new IOException("Received invalid Secret Key for request type " + requestType);
        }

        try {
            return new BootstrapRequest(requestType, args);
        } catch (Exception e) {
            throw new IOException("Received invalid request from Bootstrap; request type = " + requestType);
        }
    }
}
