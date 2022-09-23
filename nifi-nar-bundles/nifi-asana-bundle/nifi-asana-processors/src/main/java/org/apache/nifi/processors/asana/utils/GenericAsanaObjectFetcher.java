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
import com.google.gson.reflect.TypeToken;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class GenericAsanaObjectFetcher<T> extends PollableAsanaObjectFetcher {
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
            throw new RuntimeException(e);
        }
        return state;
    }

    @Override
    public void loadState(Map<String, String> state) {
        if (state.containsKey(this.getClass().getName() + LAST_FINGERPRINTS)) {
            Type type = new TypeToken<HashMap<String, String>>() {}.getType();
            try {
                lastFingerprints = Json.getInstance().fromJson(decompress(state.get(this.getClass().getName() + LAST_FINGERPRINTS)), type);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void clearState() {
        lastFingerprints.clear();
    }

    @Override
    protected Collection<AsanaObject> poll() {
        List<AsanaObject> pending = new ArrayList<>();
        Map<String, T> currentObjects = refreshObjects();

        lastFingerprints.keySet().stream()
                .filter(gid -> !currentObjects.containsKey(gid))
                .map(gid -> new AsanaObject(AsanaObjectState.REMOVED, gid))
                .forEach(pending::add);

        for (Map.Entry<String, T> entry : currentObjects.entrySet()) {
            String payload = transformObjectToPayload(entry.getValue());
            String fingerprint = Optional.ofNullable(createObjectFingerprint(entry.getValue())).orElseGet(() -> calculateSecureHash(payload));
            if (!lastFingerprints.containsKey(entry.getKey())) {
                pending.add(new AsanaObject(AsanaObjectState.NEW, entry.getKey(), payload, fingerprint));
            } else if (!lastFingerprints.get(entry.getKey()).equals(fingerprint)) {
                pending.add(new AsanaObject(AsanaObjectState.UPDATED, entry.getKey(), payload, fingerprint));
            }
        }

        return pending;
    }

    protected String transformObjectToPayload(T object) {
        return Json.getInstance().toJson(object);
    }

    protected String createObjectFingerprint(T object) {
        return null;
    }

    protected abstract Map<String, T> refreshObjects();

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
            while((n = gzip.read(buffer)) > -1) {
                uncompressedBytes.write(buffer, 0, n);
            }
        }
        return new String(uncompressedBytes.toByteArray(), StandardCharsets.UTF_8);
    }

    private String calculateSecureHash(String input) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-512").digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
