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
package org.apache.nifi.controller;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.DataOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.persistence.StandardSnippetDeserializer;
import org.apache.nifi.persistence.StandardSnippetSerializer;

public class SnippetManager {

    private final ConcurrentMap<String, StandardSnippet> snippetMap = new ConcurrentHashMap<>();

    public void addSnippet(final StandardSnippet snippet) {
        final StandardSnippet oldSnippet = this.snippetMap.putIfAbsent(snippet.getId(), snippet);
        if (oldSnippet != null) {
            throw new IllegalStateException("Snippet with ID " + snippet.getId() + " already exists");
        }
    }

    public void removeSnippet(final StandardSnippet snippet) {
        if (!snippetMap.remove(snippet.getId(), snippet)) {
            throw new IllegalStateException("Snippet is not contained in this SnippetManager");
        }
    }

    public StandardSnippet getSnippet(final String identifier) {
        return snippetMap.get(identifier);
    }

    public Collection<StandardSnippet> getSnippets() {
        return snippetMap.values();
    }

    public void clear() {
        snippetMap.clear();
    }

    public static List<StandardSnippet> parseBytes(final byte[] bytes) {
        final List<StandardSnippet> snippets = new ArrayList<>();

        try (final InputStream rawIn = new ByteArrayInputStream(bytes);
                final DataInputStream in = new DataInputStream(rawIn)) {
            final int length = in.readInt();
            final byte[] buffer = new byte[length];
            StreamUtils.fillBuffer(in, buffer, true);
            final StandardSnippet snippet = StandardSnippetDeserializer.deserialize(new ByteArrayInputStream(buffer));
            snippets.add(snippet);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to parse bytes", e);  // should never happen because of streams being used
        }

        return snippets;
    }

    public byte[] export() {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream dos = new DataOutputStream(baos)) {
            for (final StandardSnippet snippet : getSnippets()) {
                final byte[] bytes = StandardSnippetSerializer.serialize(snippet);
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }

            return baos.toByteArray();
        } catch (final IOException e) {
            // won't happen
            return null;
        }
    }
}
