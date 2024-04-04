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
package org.apache.nifi.websocket;

import java.io.UnsupportedEncodingException;

public class WebSocketMessage {
    public static final String CHARSET_NAME = "UTF-8";

    public enum Type {
        TEXT,
        BINARY
    }

    private final WebSocketSessionInfo sessionInfo;
    private byte[] payload;
    private int offset;
    private int length;
    private Type type;

    public WebSocketMessage(final WebSocketSessionInfo sessionInfo) {
        this.sessionInfo = sessionInfo;
    }

    public WebSocketSessionInfo getSessionInfo() {
        return sessionInfo;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(final String text) {
        if (text == null) {
            return;
        }

        try {
            final byte[] bytes = text.getBytes(CHARSET_NAME);
            setPayload(bytes, 0, bytes.length);
            type = Type.TEXT;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to serialize messageStr, due to " + e, e);
        }
    }

    public void setPayload(final byte[] payload, final int offset, final int length) {
        this.payload = payload;
        this.offset = offset;
        this.length = length;
        type = Type.BINARY;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public Type getType() {
        return type;
    }
}
