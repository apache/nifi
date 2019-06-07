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
package org.apache.nifi.properties;


import org.apache.nifi.properties.sensitive.SensitivePropertyValueDescriptor;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.sensitive.aes.AESSensitivePropertyProviderFactory;
import org.apache.nifi.properties.sensitive.aws.kms.AWSKMSSensitivePropertyProviderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SensitivePropertyProviderFactorySelector {
    private static final Logger logger = LoggerFactory.getLogger(SensitivePropertyProviderFactorySelector.class);

    public static SensitivePropertyProviderFactory selectProviderFactory(String value) throws SensitivePropertyProtectionException {
        return selectProviderFactory(SensitivePropertyValueDescriptor.fromValue(value));
    }

    public static SensitivePropertyProviderFactory selectProviderFactory(SensitivePropertyValueDescriptor property) throws SensitivePropertyProtectionException {
        if (AWSKMSSensitivePropertyProviderFactory.canCreate(property)) {
            logger.info("Selected AWS KMS sensitive property provider factory.");
            return new AWSKMSSensitivePropertyProviderFactory(property);
        }

        if (AESSensitivePropertyProviderFactory.canCreate(property)) {
            logger.info("Selected AES sensitive property provider factory.");
            return new AESSensitivePropertyProviderFactory(property.getPropertyValue());
        }

        throw new SensitivePropertyProtectionException("Unable to select SensitivePropertyProviderFactory.");
    }
}
