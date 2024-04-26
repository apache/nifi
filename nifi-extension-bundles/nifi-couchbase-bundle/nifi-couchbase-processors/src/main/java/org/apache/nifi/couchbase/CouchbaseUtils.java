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
package org.apache.nifi.couchbase;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.LegacyDocument;

import java.nio.charset.StandardCharsets;

public class CouchbaseUtils {

    /**
     * A convenient method to retrieve String value when Document type is unknown.
     * This method uses LegacyDocument to get, then tries to convert content based on its class.
     * @param bucket the bucket to get a document
     * @param id the id of the target document
     * @return String representation of the stored value, or null if not found
     */
    public static String getStringContent(Bucket bucket, String id) {
        final LegacyDocument doc = bucket.get(LegacyDocument.create(id));
        if (doc == null) {
            return null;
        }
        final Object content = doc.content();
        return getStringContent(content);
    }

    public static String getStringContent(Object content) {
        if (content instanceof String) {
            return (String) content;
        } else if (content instanceof byte[]) {
            return new String((byte[]) content, StandardCharsets.UTF_8);
        } else if (content instanceof ByteBuf) {
            final ByteBuf byteBuf = (ByteBuf) content;
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(bytes);
            byteBuf.release();
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return content.toString();
    }

}
