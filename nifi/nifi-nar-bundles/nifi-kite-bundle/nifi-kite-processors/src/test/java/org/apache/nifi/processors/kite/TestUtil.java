/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;

public class TestUtil {

    public static final Schema USER_SCHEMA = SchemaBuilder.record("User").fields()
            .requiredString("username")
            .requiredString("email")
            .endRecord();

    public static Record user(String username, String email) {
        Record user = new Record(USER_SCHEMA);
        user.put("username", username);
        user.put("email", email);
        return user;
    }

    public static InputStream streamFor(Record... records) throws IOException {
        return streamFor(Arrays.asList(records));
    }

    public static InputStream streamFor(List<Record> records) throws IOException {
        return new ByteArrayInputStream(bytesFor(records));
    }

    public static InputStream invalidStreamFor(Record... records) throws IOException {
        return invalidStreamFor(Arrays.asList(records));
    }

    public static InputStream invalidStreamFor(List<Record> records) throws IOException {
        // purposely truncate the content
        byte[] bytes = bytesFor(records);
        return new ByteArrayInputStream(bytes, 0, bytes.length / 2);
    }

    private static byte[] bytesFor(List<Record> records) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataFileWriter<Record> writer = new DataFileWriter<>(
                AvroUtil.newDatumWriter(records.get(0).getSchema(), Record.class));
        writer.setCodec(CodecFactory.snappyCodec());
        writer = writer.create(records.get(0).getSchema(), out);

        for (Record record : records) {
            writer.append(record);
        }

        writer.flush();

        return out.toByteArray();
    }

    public static InputStream streamFor(String content) throws CharacterCodingException {
        return streamFor(content, Charset.forName("utf8"));
    }

    public static InputStream streamFor(String content, Charset charset) throws CharacterCodingException {
        return new ByteArrayInputStream(bytesFor(content, charset));
    }

    public static byte[] bytesFor(String content, Charset charset) throws CharacterCodingException {
        CharBuffer chars = CharBuffer.wrap(content);
        CharsetEncoder encoder = charset.newEncoder();
        ByteBuffer buffer = encoder.encode(chars);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

}
