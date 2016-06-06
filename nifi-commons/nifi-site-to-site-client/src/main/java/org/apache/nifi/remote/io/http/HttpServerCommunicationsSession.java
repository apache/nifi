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
package org.apache.nifi.remote.io.http;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.protocol.ResponseCode;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class HttpServerCommunicationsSession extends HttpCommunicationsSession {

    private final Map<String, String> handshakeParams = new HashMap<>();
    private final String transactionId;
    private Transaction.TransactionState status = Transaction.TransactionState.TRANSACTION_STARTED;
    private ResponseCode responseCode;

    public HttpServerCommunicationsSession(InputStream inputStream, OutputStream outputStream, String transactionId){
        super();
        input.setInputStream(inputStream);
        output.setOutputStream(outputStream);
        this.transactionId = transactionId;
    }

    // This status is only needed by HttpFlowFileServerProtocol, HttpClientTransaction has its own status.
    // Because multiple HttpFlowFileServerProtocol instances have to carry on a single transaction
    // throughout multiple HTTP requests, status has to be embedded here.
    public Transaction.TransactionState getStatus() {
        return status;
    }

    public void setStatus(Transaction.TransactionState status) {
        this.status = status;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(ResponseCode responseCode) {
        this.responseCode = responseCode;
    }

    public void putHandshakeParam(HandshakeProperty key, String value) {
        handshakeParams.put(key.name(), value);
    }

    public Map<String, String> getHandshakeParams() {
        return handshakeParams;
    }
}
