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
package org.apache.nifi.smb.common;

import com.hierynomus.mssmb2.SMB2Dialect;
import com.hierynomus.smbj.SmbConfig;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockPropertyContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hierynomus.mssmb2.SMB2Dialect.SMB_2_0_2;
import static com.hierynomus.mssmb2.SMB2Dialect.SMB_2_1;
import static com.hierynomus.mssmb2.SMB2Dialect.SMB_3_0;
import static com.hierynomus.mssmb2.SMB2Dialect.SMB_3_0_2;
import static com.hierynomus.mssmb2.SMB2Dialect.SMB_3_1_1;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SmbUtilsTest {

    @Test
    public void testSmbConfigDefault() {
        Map<PropertyDescriptor, String> properties = new HashMap<>();

        properties.put(SmbProperties.SMB_DIALECT, null);
        properties.put(SmbProperties.USE_ENCRYPTION, null);
        properties.put(SmbProperties.TIMEOUT, null);

        MockPropertyContext propertyContext = new MockPropertyContext(properties);

        SmbConfig config = SmbUtils.buildSmbConfig(propertyContext);

        assertSmbConfig(config, new HashSet<>(Arrays.asList(SMB_3_1_1, SMB_3_0_2, SMB_3_0, SMB_2_1, SMB_2_0_2)), false, 5_000);
    }

    @Test
    public void testSmbConfigNonDefault() {
        Map<PropertyDescriptor, String> properties = new HashMap<>();

        properties.put(SmbProperties.SMB_DIALECT, SmbDialect.SMB_3_1_1.getValue());
        properties.put(SmbProperties.USE_ENCRYPTION, "true");
        properties.put(SmbProperties.TIMEOUT, "30 s");

        MockPropertyContext propertyContext = new MockPropertyContext(properties);

        SmbConfig config = SmbUtils.buildSmbConfig(propertyContext);

        assertSmbConfig(config, Collections.singleton(SMB_3_1_1), true, 30_000);
    }

    private void assertSmbConfig(SmbConfig config, Set<SMB2Dialect> expectedDialects, boolean expectedEncryption, long expectedTimeout) {
        assertEquals(expectedDialects, config.getSupportedDialects());

        assertEquals(expectedEncryption, config.isEncryptData());

        assertEquals(expectedTimeout, config.getReadTimeout());
        assertEquals(expectedTimeout, config.getWriteTimeout());
        assertEquals(expectedTimeout, config.getTransactTimeout());
    }
}
