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
package org.apache.nifi.services.couchbase.utils;

import com.couchbase.client.java.codec.RawBinaryTranscoder;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.codec.Transcoder;

import java.util.function.Predicate;

/**
 * Couchbase document types.
 */
public enum DocumentType {

    Json(RawJsonTranscoder.INSTANCE, new JsonValidator(), "application/json"),
    Binary(RawBinaryTranscoder.INSTANCE, v -> true, "application/octet-stream");

    private final Transcoder transcoder;
    private final Predicate<byte[]> validator;
    private final String mimeType;

    DocumentType(Transcoder transcoder, Predicate<byte[]> validator, String mimeType) {
        this.transcoder = transcoder;
        this.validator = validator;
        this.mimeType = mimeType;
    }

    public Transcoder getTranscoder() {
        return transcoder;
    }

    public String getMimeType() {
        return mimeType;
    }

    public Predicate<byte[]> getValidator() {
        return validator;
    }
}
