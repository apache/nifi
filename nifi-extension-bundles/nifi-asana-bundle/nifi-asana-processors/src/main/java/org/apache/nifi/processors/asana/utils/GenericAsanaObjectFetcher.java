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
package org.apache.nifi.processors.asana.utils;

import com.asana.Json;
import com.asana.models.Resource;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections4.iterators.FilterIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.util.Collections.emptySet;

public abstract class GenericAsanaObjectFetcher<T extends Resource> extends AbstractAsanaObjectFetcher {
    private static final String LAST_FINGERPRINTS = ".lastFingerprints";

    private Map<String, String> lastFingerprints;

    public GenericAsanaObjectFetcher() {
        super();
        this.lastFingerprints = new HashMap<>();
    }

    @Override
    public AsanaObject fetchNext() {
        AsanaObject result = super.fetchNext();
        if (result != null) {
            if (result.getState().equals(AsanaObjectState.REMOVED)) {
                lastFingerprints.remove(result.getGid());
            } else {
                lastFingerprints.put(result.getGid(), result.getFingerprint());
            }
        }
        return result;
    }

    @Override
    public Map<String, String> saveState() {
        Map<String, String> state = new HashMap<>();
        try {
            state.put(this.getClass().getName() + LAST_FINGERPRINTS, compress(Json.getInstance().toJson(lastFingerprints)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return state;
    }

    @Override
    public void loadState(Map<String, String> state) {
        if (state.containsKey(this.getClass().getName() + LAST_FINGERPRINTS)) {
            Type type = new TypeToken<HashMap<String, String>>() { }.getType();
            try {
                lastFingerprints = Json.getInstance().fromJson(decompress(state.get(this.getClass().getName() + LAST_FINGERPRINTS)), type);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void clearState() {
        super.clearState();
        lastFingerprints.clear();
    }

    @Override
    protected Iterator<AsanaObject> fetch() {
        Stream<AsanaObject> currentObjects = fetchObjects()
                .map(item -> {
                    String payload = transformObjectToPayload(item);
                    return new AsanaObject(
                            lastFingerprints.containsKey(item.gid) ? AsanaObjectState.UPDATED : AsanaObjectState.NEW,
                            item.gid,
                            payload,
                            Optional.ofNullable(createObjectFingerprint(item)).orElseGet(() -> calculateSecureHash(payload)));
                });

        return new FilterIterator<>(
                new Iterator<>() {
                    Iterator<AsanaObject> it = currentObjects.iterator();
                    Set<String> unseenIds = new HashSet<>(lastFingerprints.keySet()); // copy all previously seen ids.

                    @Override
                    public boolean hasNext() {
                        return it.hasNext() || !unseenIds.isEmpty();
                    }

                    @Override
                    public AsanaObject next() {
                        if (it.hasNext()) {
                            AsanaObject next = it.next();
                            unseenIds.remove(next.getGid());
                            return next;
                        }
                        it = unseenIds.stream()
                                .map(gid -> new AsanaObject(AsanaObjectState.REMOVED, gid,
                                        Json.getInstance().toJson(gid)))
                                .iterator();
                        unseenIds = emptySet();
                        return it.next();
                    }
                },
                item -> !item.getState().equals(AsanaObjectState.UPDATED) || !lastFingerprints.get(item.getGid())
                        .equals(item.getFingerprint())
        );
    }

    protected String transformObjectToPayload(T object) {
        return Json.getInstance().toJson(object);
    }

    protected String createObjectFingerprint(T object) {
        return null;
    }

    protected abstract Stream<T> fetchObjects();

    private static String compress(String str) throws IOException {
        ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream(str.length());
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressedBytes)) {
            gzip.write(str.getBytes(StandardCharsets.UTF_8));
        }
        return Base64.getEncoder().encodeToString(compressedBytes.toByteArray());
    }

    private static String decompress(String str) throws IOException {
        ByteArrayOutputStream uncompressedBytes = new ByteArrayOutputStream();
        try (InputStream gzip = new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(str)))) {
            int n;
            byte[] buffer = new byte[1024];
            while ((n = gzip.read(buffer)) > -1) {
                uncompressedBytes.write(buffer, 0, n);
            }
        }
        return uncompressedBytes.toString(StandardCharsets.UTF_8);
    }

    private String calculateSecureHash(String input) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-512").digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new AsanaObjectFetcherException(e);
        }
    }
}
