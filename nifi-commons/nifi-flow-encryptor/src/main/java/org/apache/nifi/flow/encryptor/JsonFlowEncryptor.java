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
package org.apache.nifi.flow.encryptor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.encrypt.PropertyEncryptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.regex.Matcher;

public class JsonFlowEncryptor extends AbstractFlowEncryptor {
    @Override
    public void processFlow(final InputStream inputStream, final OutputStream outputStream,
                            final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) {
        final JsonFactory factory = new JsonFactory();
        try (final JsonGenerator generator = factory.createGenerator(outputStream)){
            try (final JsonParser parser = factory.createParser(inputStream)) {
                parser.setCodec(new ObjectMapper());
                processJsonByTokens(parser, generator, inputEncryptor, outputEncryptor);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed Processing Flow Configuration", e);
        }
    }

    private void processJsonByTokens(final JsonParser parser, final JsonGenerator generator,
                                     final PropertyEncryptor inputEncryptor, final PropertyEncryptor outputEncryptor) throws IOException {
        JsonToken token = parser.nextToken();
        while (token != null) {
            switch (token) {
                case NOT_AVAILABLE:
                    break;
                case START_OBJECT:
                    generator.writeStartObject();
                    break;
                case END_OBJECT:
                    generator.writeEndObject();
                    break;
                case START_ARRAY:
                    generator.writeStartArray();
                    break;
                case END_ARRAY:
                    generator.writeEndArray();
                    break;
                case FIELD_NAME:
                    generator.writeFieldName(parser.getValueAsString());
                    break;
                case VALUE_EMBEDDED_OBJECT:
                    generator.writeEmbeddedObject(parser.getEmbeddedObject());
                    break;
                case VALUE_STRING:
                    final String value = parser.getValueAsString();
                    final Matcher matcher = ENCRYPTED_PATTERN.matcher(value);
                    if (matcher.matches()) {
                        generator.writeString(getOutputEncrypted(matcher.group(FIRST_GROUP), inputEncryptor, outputEncryptor));
                    } else {
                        generator.writeString(value);
                    }
                    break;
                case VALUE_NUMBER_INT:
                    generator.writeNumber(parser.getIntValue());
                    break;
                case VALUE_NUMBER_FLOAT:
                    generator.writeRawValue(parser.getValueAsString());
                    break;
                case VALUE_TRUE:
                    generator.writeBoolean(true);
                    break;
                case VALUE_FALSE:
                    generator.writeBoolean(false);
                    break;
                case VALUE_NULL:
                    generator.writeNull();
                    break;
                default:
                    throw new IllegalStateException(String.format("Token unrecognized [%s]", token));
            }
            token = parser.nextToken();
        }
    }
}
