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

package org.apache.nifi.cluster.coordination.http.replication.okhttp;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonEntitySerializer implements EntitySerializer {
    private final ObjectMapper jsonCodec;

    public JsonEntitySerializer(final ObjectMapper jsonCodec) {
        this.jsonCodec = jsonCodec;
    }

    @Override
    public void serialize(final Object entity, final OutputStream out) throws IOException {
        final JsonFactory factory = new JsonFactory();
        final JsonGenerator generator = factory.createGenerator(out);
        generator.setCodec(jsonCodec);
        generator.writeObject(entity);
        generator.flush();
    }
}
