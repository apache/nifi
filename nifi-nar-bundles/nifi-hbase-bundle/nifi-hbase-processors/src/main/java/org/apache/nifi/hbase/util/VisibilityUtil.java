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

package org.apache.nifi.hbase.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;

public class VisibilityUtil {
    public static String pickVisibilityString(String columnFamily, String columnQualifier, FlowFile flowFile, ProcessContext context) {
        if (StringUtils.isBlank(columnFamily)) {
            return null;
        }
        String lookupKey = String.format("visibility.%s%s%s", columnFamily, !StringUtils.isBlank(columnQualifier) ? "." : "", columnQualifier);
        String fromAttribute = flowFile.getAttribute(lookupKey);

        if (fromAttribute == null && !StringUtils.isBlank(columnQualifier)) {
            String lookupKeyFam = String.format("visibility.%s", columnFamily);
            fromAttribute = flowFile.getAttribute(lookupKeyFam);
        }

        if (fromAttribute != null) {
            return fromAttribute;
        } else {
            PropertyValue descriptor = context.getProperty(lookupKey);
            if (descriptor == null || !descriptor.isSet()) {
                descriptor = context.getProperty(String.format("visibility.%s", columnFamily));
            }

            String retVal = descriptor != null ? descriptor.evaluateAttributeExpressions(flowFile).getValue() : null;

            return retVal;
        }
    }
}
