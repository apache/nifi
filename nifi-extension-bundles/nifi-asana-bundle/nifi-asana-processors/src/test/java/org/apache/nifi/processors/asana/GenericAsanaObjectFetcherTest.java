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
package org.apache.nifi.processors.asana;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.asana.models.Resource;
import com.google.gson.Gson;
import java.util.Map;
import org.apache.nifi.processors.asana.mocks.MockGenericAsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.junit.jupiter.api.Test;

public class GenericAsanaObjectFetcherTest {

    private static final Gson GSON = new Gson();

    @Test
    public void testNoObjects() {
        final MockGenericAsanaObjectFetcher fetcher = new MockGenericAsanaObjectFetcher();
        fetcher.items = emptyList();

        assertNull(fetcher.fetchNext());
        assertEquals(1, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(2, fetcher.refreshCount);
    }

    @Test
    public void testSingleStaticObject() {
        final MockGenericAsanaObjectFetcher fetcher = new MockGenericAsanaObjectFetcher();

        final Resource resource = new Resource();
        resource.gid = "123";
        resource.resourceType = "Something";

        fetcher.items = singletonList(resource);

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals("123", object.getGid());
        assertFalse(object.getContent().isEmpty());
        assertEquals(1, fetcher.refreshCount);
        assertNull(fetcher.fetchNext());

        assertNull(fetcher.fetchNext());
        assertEquals(2, fetcher.refreshCount);
    }

    @Test
    public void testSingleObjectAddedUpdatedRemoved() {
        final MockGenericAsanaObjectFetcher fetcher = new MockGenericAsanaObjectFetcher();

        final Resource resource = new Resource();
        resource.gid = "123";
        resource.resourceType = "Something";

        fetcher.items = singletonList(resource);

        final AsanaObject objectWhenNew = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, objectWhenNew.getState());
        assertEquals("123", objectWhenNew.getGid());
        assertFalse(objectWhenNew.getContent().isEmpty());
        assertEquals(1, fetcher.refreshCount);
        assertNull(fetcher.fetchNext());

        resource.resourceType = "Etwas";
        final AsanaObject objectAfterUpdate = fetcher.fetchNext();
        assertEquals(AsanaObjectState.UPDATED, objectAfterUpdate.getState());
        assertEquals("123", objectAfterUpdate.getGid());
        assertFalse(objectAfterUpdate.getContent().isEmpty());
        assertNotEquals(objectWhenNew.getContent(), objectAfterUpdate.getContent());
        assertNotEquals(objectWhenNew.getFingerprint(), objectAfterUpdate.getFingerprint());
        assertEquals(2, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(2, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(3, fetcher.refreshCount);

        resource.resourceType = "Something";
        final AsanaObject objectAfterAnotherUpdate = fetcher.fetchNext();
        assertEquals(AsanaObjectState.UPDATED, objectAfterAnotherUpdate.getState());
        assertEquals("123", objectAfterAnotherUpdate.getGid());
        assertFalse(objectAfterAnotherUpdate.getContent().isEmpty());
        assertNotEquals(objectAfterUpdate.getContent(), objectAfterAnotherUpdate.getContent());
        assertNotEquals(objectAfterUpdate.getFingerprint(), objectAfterAnotherUpdate.getFingerprint());
        assertEquals(4, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(4, fetcher.refreshCount);

        fetcher.items = emptyList();
        final AsanaObject objectAfterRemove = fetcher.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, objectAfterRemove.getState());
        assertEquals("123", objectAfterRemove.getGid());
        assertEquals(GSON.toJson(objectAfterRemove.getGid()), objectAfterRemove.getContent());
        assertEquals(5, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(5, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(6, fetcher.refreshCount);
    }

    @Test
    public void testSingleObjectIdChange() {
        final MockGenericAsanaObjectFetcher fetcher = new MockGenericAsanaObjectFetcher();

        final Resource resource = new Resource();
        resource.gid = "123";
        resource.resourceType = "Something";

        fetcher.items = singletonList(resource);

        final AsanaObject objectBeforeIdChange = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, objectBeforeIdChange.getState());
        assertEquals("123", objectBeforeIdChange.getGid());
        assertFalse(objectBeforeIdChange.getContent().isEmpty());
        assertEquals(1, fetcher.refreshCount);
        assertNull(fetcher.fetchNext());

        resource.gid = "456";

        final AsanaObject objectWithNewId = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, objectWithNewId.getState());
        assertEquals("456", objectWithNewId.getGid());
        assertFalse(objectWithNewId.getContent().isEmpty());
        assertEquals(2, fetcher.refreshCount);

        final AsanaObject objectAfterIdChange = fetcher.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, objectAfterIdChange.getState());
        assertEquals("123", objectAfterIdChange.getGid());
        assertEquals(GSON.toJson(objectAfterIdChange.getGid()), objectAfterIdChange.getContent());
        assertEquals(2, fetcher.refreshCount);

        assertNull(fetcher.fetchNext());
        assertEquals(2, fetcher.refreshCount);
    }

    @Test
    public void testStateSavedAndThenRestored() {
        final MockGenericAsanaObjectFetcher fetcher1 = new MockGenericAsanaObjectFetcher();

        final Resource resource = new Resource();
        resource.gid = "123";
        resource.resourceType = "Something";

        fetcher1.items = singletonList(resource);

        final AsanaObject objectBeforeStateExport = fetcher1.fetchNext();
        assertEquals(AsanaObjectState.NEW, objectBeforeStateExport.getState());
        assertEquals("123", objectBeforeStateExport.getGid());

        final Map<String, String> savedState = fetcher1.saveState();

        final MockGenericAsanaObjectFetcher fetcher2 = new MockGenericAsanaObjectFetcher();

        fetcher2.loadState(savedState);

        fetcher2.items = singletonList(resource);
        assertNull(fetcher2.fetchNext());

        fetcher2.items = emptyList();
        final AsanaObject objectAfterStateImport = fetcher2.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, objectAfterStateImport.getState());
        assertEquals("123", objectAfterStateImport.getGid());
    }

    @Test
    public void testClearState() {
        final MockGenericAsanaObjectFetcher fetcher = new MockGenericAsanaObjectFetcher();

        final Resource resource = new Resource();
        resource.gid = "123";
        resource.resourceType = "Something";

        fetcher.items = singletonList(resource);

        final AsanaObject objectBeforeStateCleared = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, objectBeforeStateCleared.getState());
        assertEquals("123", objectBeforeStateCleared.getGid());

        fetcher.clearState();

        final AsanaObject objectAfterStateCleared = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, objectAfterStateCleared.getState());
        assertEquals("123", objectAfterStateCleared.getGid());
    }
}
