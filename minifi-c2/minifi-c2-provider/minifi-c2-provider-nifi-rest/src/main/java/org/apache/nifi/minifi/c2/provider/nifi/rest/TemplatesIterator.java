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

package org.apache.nifi.minifi.c2.provider.nifi.rest;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class TemplatesIterator implements Iterator<Pair<String, String>>, Closeable {
    public static final String FLOW_TEMPLATES = "/flow/templates";

    private final HttpURLConnection urlConnection;
    private final InputStream inputStream;
    private final JsonParser parser;
    private Pair<String, String> next;

    public TemplatesIterator(NiFiRestConnector niFiRestConnector, JsonFactory jsonFactory) throws ConfigurationProviderException, IOException {
        urlConnection = niFiRestConnector.get(FLOW_TEMPLATES);
        inputStream = urlConnection.getInputStream();
        parser = jsonFactory.createParser(inputStream);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if ("templates".equals(parser.getCurrentName())) {
                break;
            }
        }
        next = getNext();
    }

    private Pair<String, String> getNext() throws IOException {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            if ("template".equals(parser.getCurrentName())) {
                String id = null;
                String name = null;
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String currentName = parser.getCurrentName();
                    if ("id".equals(currentName)) {
                        parser.nextToken();
                        id = parser.getText();
                    } else if ("name".equals(currentName)) {
                        parser.nextToken();
                        name = parser.getText();
                    }
                }
                return new Pair<>(id, name);
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Pair<String, String> next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        try {
            return next;
        } finally {
            try {
                next = getNext();
            } catch (IOException e) {
                throw new TemplatesIteratorException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (parser != null) {
            try {
                parser.close();
            } catch (IOException e) {
                //Ignore
            }
        }
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                //Ignore
            }
        }
        if (urlConnection != null) {
            urlConnection.disconnect();
        }
    }
}
