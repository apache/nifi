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
package org.apache.nifi.processor.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import org.junit.Test;

public class TestStandardValidators {

    @Test
    public void testTimePeriodValidator() {
        Validator val = StandardValidators.createTimePeriodValidator(1L, TimeUnit.SECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        ValidationResult vr;

        vr = val.validate("TimePeriodTest", "0 sense made", null);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", null, null);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "0 secs", null);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "999 millis", null);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "999999999 nanos", null);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "1 sec", null);
        assertTrue(vr.isValid());
    }
    
    @Test
    public void testDataSizeBoundsValidator() {
        Validator val = StandardValidators.createDataSizeBoundsValidator(100, 1000);
        ValidationResult vr; 
        
        vr = val.validate("DataSizeBounds", "5 GB", null);
        assertFalse(vr.isValid());
        
        vr = val.validate("DataSizeBounds", "0 B", null);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "99 B", null);
        assertFalse(vr.isValid());
        
        vr = val.validate("DataSizeBounds", "100 B", null);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "999 B", null);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "1000 B", null);
        assertTrue(vr.isValid());
        
        vr = val.validate("DataSizeBounds", "1001 B", null);
        assertFalse(vr.isValid());
        
        vr = val.validate("DataSizeBounds", "water", null);
        assertFalse(vr.isValid());
        
    }
}
