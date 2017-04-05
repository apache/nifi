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
package org.apache.nifi.cdc.mysql.event;

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


/**
 * Unit Tests for MySQLCDCUtils utility class
 */
public class MySQLCDCUtilsTest {
    @Test
    public void testGetWritableObject() throws Exception {
        assertNull(MySQLCDCUtils.getWritableObject(null, null));
        assertNull(MySQLCDCUtils.getWritableObject(Types.INTEGER, null));
        assertEquals((byte) 1, MySQLCDCUtils.getWritableObject(Types.INTEGER, (byte) 1));
        assertEquals("Hello", MySQLCDCUtils.getWritableObject(Types.VARCHAR, "Hello".getBytes()));
    }

}