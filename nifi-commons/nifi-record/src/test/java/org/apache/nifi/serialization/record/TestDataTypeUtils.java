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

package org.apache.nifi.serialization.record;

import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

public class TestDataTypeUtils {
    /**
     * This is a unit test to verify conversion java Date objects to Timestamps. Support for this was
     * required in order to help the MongoDB packages handle date/time logical types in the Record API.
     */
    @Test
    public void testDateToTimestamp() {
        java.util.Date date = new java.util.Date();
        Timestamp ts = DataTypeUtils.toTimestamp(date, null, null);

        Assert.assertNotNull(ts);
        Assert.assertEquals("Times didn't match", ts.getTime(), date.getTime());

        java.sql.Date sDate = new java.sql.Date(date.getTime());
        ts = DataTypeUtils.toTimestamp(date, null, null);
        Assert.assertNotNull(ts);
        Assert.assertEquals("Times didn't match", ts.getTime(), sDate.getTime());
    }
}
