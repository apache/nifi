/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import org.apache.nifi.components.ValidationResult;
import org.junit.Assert;
import org.junit.Test;

public class TestInfluxLineProtocolReaderSettings extends AbstractTestInfluxLineProtocolReader {

    @Test
    public void defaultSettingsIsValid() {

        testRunner.assertValid(readerFactory);
    }

    @Test
    public void encodingNull() {

        testRunner.disableControllerService(readerFactory);

        ValidationResult result = testRunner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, (String) null);

        Assert.assertFalse(result.isValid());
    }

    @Test
    public void encodingNotValid() {

        testRunner.disableControllerService(readerFactory);

        ValidationResult result = testRunner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, "not-valid");

        Assert.assertFalse(result.isValid());
    }
}
