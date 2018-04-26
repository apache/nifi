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
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;

public class PutMongoTest {

    /*
     * Corresponds to NIFI-5047
     */
    @Test
    public void testQueryKeyValidation() {
        TestRunner runner = TestRunners.newTestRunner(PutMongo.class);
        runner.setProperty(PutMongo.URI, "mongodb://localhost:27017");
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
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Iterator<ValidationResult> it = results.iterator();
        Assert.assertTrue(it.next().toString().endsWith("Both update query key and update query cannot be set at the same time."));

        runner.removeProperty(PutMongo.UPDATE_QUERY);
        runner.removeProperty(PutMongo.UPDATE_QUERY_KEY);

        runner.assertNotValid();

        results = null;
        if (runner.getProcessContext() instanceof MockProcessContext) {
            results = ((MockProcessContext) runner.getProcessContext()).validate();
        }

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        it = results.iterator();
        Assert.assertTrue(it.next().toString().endsWith("Either the update query key or the update query field must be set."));
    }
}
