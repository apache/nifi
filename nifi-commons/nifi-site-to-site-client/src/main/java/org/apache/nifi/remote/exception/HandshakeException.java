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
package org.apache.nifi.remote.exception;

import org.apache.nifi.remote.protocol.ResponseCode;

import java.io.IOException;

/**
 * A HandshakeException occurs when the client and the remote NiFi instance do
 * not agree on some condition during the handshake. For example, if the NiFi
 * instance does not recognize one of the parameters that the client passes
 * during the Handshaking phase.
 */
public class HandshakeException extends IOException {

    private static final long serialVersionUID = 178192341908726L;

    private final ResponseCode responseCode;

    public HandshakeException(final String message) {
        super(message);
        this.responseCode = null;
    }

    public HandshakeException(final Throwable cause) {
        super(cause);
        this.responseCode = null;
    }

    public HandshakeException(final ResponseCode responseCode, final String message) {
        super(message);
        this.responseCode = responseCode;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }
}
