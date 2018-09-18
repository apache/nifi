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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.persistence.StandardSnippetDeserializer;
import org.apache.nifi.persistence.StandardSnippetSerializer;
import org.apache.nifi.stream.io.StreamUtils;

public class SnippetManager {

    private final Cache<String, StandardSnippet> snippetMap = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();

    public synchronized void addSnippet(final StandardSnippet snippet) {
        if (snippetMap.getIfPresent(snippet.getId()) != null) {
            throw new IllegalStateException("Snippet with ID " + snippet.getId() + " already exists");
        }
        snippetMap.put(snippet.getId(), snippet);
    }

    public synchronized void removeSnippet(final StandardSnippet snippet) {
        if (snippetMap.getIfPresent(snippet.getId()) == null) {
            throw new IllegalStateException("Snippet is not contained in this SnippetManager");
        }
        snippetMap.invalidate(snippet.getId());
    }

    public synchronized StandardSnippet getSnippet(final String identifier) {
        return snippetMap.getIfPresent(identifier);
    }

    public synchronized Collection<StandardSnippet> getSnippets() {
        return Collections.unmodifiableCollection(snippetMap.asMap().values());
    }

    public synchronized void clear() {
        snippetMap.invalidateAll();
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
