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
package org.apache.nifi.remote.protocol;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.nifi.remote.exception.ProtocolException;

public class Response {

    private final ResponseCode code;
    private final String message;

    private Response(final ResponseCode code, final String explanation) {
        this.code = code;
        this.message = explanation;
    }

    public ResponseCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static Response read(final DataInputStream in) throws IOException, ProtocolException {
        final ResponseCode code = ResponseCode.readCode(in);
        final String message = code.containsMessage() ? in.readUTF() : null;
        return new Response(code, message);
    }

    @Override
    public String toString() {
        return code + ": " + message;
    }
}
