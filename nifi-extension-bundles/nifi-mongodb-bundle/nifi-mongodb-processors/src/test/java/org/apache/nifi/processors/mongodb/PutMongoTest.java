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

package org.apache.nifi.processors.mongodb;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutMongoTest {

    /*
     * Corresponds to NIFI-5047
     */
    @Test
    public void testQueryKeyValidation() {
        TestRunner runner = TestRunners.newTestRunner(PutMongo.class);
        runner.setProperty(PutMongo.DATABASE_NAME, "demo");
        runner.setProperty(PutMongo.COLLECTION_NAME, "messages");
        runner.setProperty(PutMongo.MODE, PutMongo.MODE_INSERT);
        runner.assertValid();

        runner.setProperty(PutMongo.MODE, PutMongo.MODE_UPDATE);
        runner.setProperty(PutMongo.UPDATE_QUERY, "{}");
        runner.setProperty(PutMongo.UPDATE_QUERY_KEY, "test");
        runner.assertNotValid();

        Collection<ValidationResult> results = null;
        if (runner.getProcessContext() instanceof MockProcessContext) {
            results = ((MockProcessContext) runner.getProcessContext()).validate();
        }
        assertNotNull(results);
        assertEquals(1, results.size());
        Iterator<ValidationResult> it = results.iterator();
        assertTrue(it.next().toString().endsWith("Both update query key and update query cannot be set at the same time."));

        runner.removeProperty(PutMongo.UPDATE_QUERY);
        runner.removeProperty(PutMongo.UPDATE_QUERY_KEY);

        runner.assertNotValid();

        results = null;
        if (runner.getProcessContext() instanceof MockProcessContext) {
            results = ((MockProcessContext) runner.getProcessContext()).validate();
        }

        assertNotNull(results);
        assertEquals(1, results.size());
        it = results.iterator();
        assertTrue(it.next().toString().endsWith("Either the update query key or the update query field must be set."));
    }

    @Test
    public void testParseUpdateKey_IdVariousTypes() throws Exception {
        PutMongo processor = new PutMongo();

        Method parseUpdateKey = PutMongo.class.getDeclaredMethod("parseUpdateKey", String.class, java.util.Map.class);
        parseUpdateKey.setAccessible(true);


        //  _id as Document should be preserved as Document
        Document embeddedId = new Document("a", 1);
        Document docWithDocId = new Document("_id", embeddedId);
        Document result3 = (Document) parseUpdateKey.invoke(processor, "_id", docWithDocId);
        assertInstanceOf(Document.class, result3.get("_id"));
        assertSame(embeddedId, result3.get("_id"));

        //  _id as List should be preserved as List
        List<Integer> listId = new ArrayList<>();
        listId.add(1);
        listId.add(2);
        Document docWithListId = new Document("_id", listId);
        Document result4 = (Document) parseUpdateKey.invoke(processor, "_id", docWithListId);
        assertInstanceOf(List.class, result4.get("_id"));
        assertSame(listId, result4.get("_id"));

        //  _id as Number should be preserved as Number
        Integer numericId = 42;
        Document docWithNumericId = new Document("_id", numericId);
        Document result5 = (Document) parseUpdateKey.invoke(processor, "_id", docWithNumericId);
        assertInstanceOf(Integer.class, result5.get("_id"));
        assertEquals(numericId, result5.get("_id"));
    }
}
