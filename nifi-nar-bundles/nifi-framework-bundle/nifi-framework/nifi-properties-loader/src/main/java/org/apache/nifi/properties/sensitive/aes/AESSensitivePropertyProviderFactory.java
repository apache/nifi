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
package org.apache.nifi.properties.sensitive.aes;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.SensitivePropertyValueDescriptor;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.SensitivePropertyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AESSensitivePropertyProviderFactory implements SensitivePropertyProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyProviderFactory.class);

    private String keyHex;

    public AESSensitivePropertyProviderFactory(String key) {
        this.keyHex = key;
    }

    public AESSensitivePropertyProviderFactory(SensitivePropertyValueDescriptor prop) {
        this.keyHex = prop.getPropertyValue();
    }

    public SensitivePropertyProvider getProvider() throws SensitivePropertyProtectionException {
        if (keyHex != null && !StringUtils.isBlank(keyHex)) {
            return new AESSensitivePropertyProvider(keyHex);
        } else {
            throw new SensitivePropertyProtectionException("The provider factory cannot generate providers without a key");
        }
    }

    @Override
    public String toString() {
        return "SensitivePropertyProviderFactory for creating AESSensitivePropertyProviders";
    }

    /**
     * For current and backward compatibility with the code in the TLS Toolkit, this implementation returns
     * `true` for all calls.  This should change with NIFI-6363.
     *
     * @param property Any property name.
     * @return true.
     */
    public static boolean canCreate(SensitivePropertyValueDescriptor property) {
        return true;
    }
}
