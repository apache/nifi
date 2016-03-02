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

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.junit.Test;
import org.mockito.Mockito;

public class TestStandardValidators {

    @Test
    public void testTimePeriodValidator() {
        Validator val = StandardValidators.createTimePeriodValidator(1L, TimeUnit.SECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);

        vr = val.validate("TimePeriodTest", "0 sense made", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", null, validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "0 secs", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "999 millis", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "999999999 nanos", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("TimePeriodTest", "1 sec", validationContext);
        assertTrue(vr.isValid());
    }

    @Test
    public void testDataSizeBoundsValidator() {
        Validator val = StandardValidators.createDataSizeBoundsValidator(100, 1000);
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        vr = val.validate("DataSizeBounds", "5 GB", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "0 B", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "99 B", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "100 B", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "999 B", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "1000 B", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("DataSizeBounds", "1001 B", validationContext);
        assertFalse(vr.isValid());

        vr = val.validate("DataSizeBounds", "water", validationContext);
        assertFalse(vr.isValid());

    }

    @Test
    public void testKerbPrincipalValidator() {
        Validator val = StandardValidators.KERB_PRINC_VALIDATOR;
        ValidationResult vr;

        final ValidationContext validationContext = Mockito.mock(ValidationContext.class);
        vr = val.validate("Kerberos Principal","jon@CDH.PROD", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("Kerberos Principal","jon@CDH", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("kerberos-principal","service/nifi@PROD", validationContext);
        assertTrue(vr.isValid());

        vr = val.validate("keberos-principal", "joewitt", validationContext);
        assertFalse(vr.isValid());
    }
}
