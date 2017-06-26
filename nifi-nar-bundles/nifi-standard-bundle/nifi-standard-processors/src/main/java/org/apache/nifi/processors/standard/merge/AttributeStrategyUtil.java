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

package org.apache.nifi.processors.standard.merge;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;

public class AttributeStrategyUtil {

    public static final AllowableValue ATTRIBUTE_STRATEGY_ALL_COMMON = new AllowableValue("Keep Only Common Attributes", "Keep Only Common Attributes",
        "Any attribute that is not the same on all FlowFiles in a bin will be dropped. Those that are the same across all FlowFiles will be retained.");
    public static final AllowableValue ATTRIBUTE_STRATEGY_ALL_UNIQUE = new AllowableValue("Keep All Unique Attributes", "Keep All Unique Attributes",
        "Any attribute that has the same value for all FlowFiles in a bin, or has no value for a FlowFile, will be kept. For example, if a bin consists of 3 FlowFiles "
            + "and 2 of them have a value of 'hello' for the 'greeting' attribute and the third FlowFile has no 'greeting' attribute then the outbound FlowFile will get "
            + "a 'greeting' attribute with the value 'hello'.");

    public static final PropertyDescriptor ATTRIBUTE_STRATEGY = new PropertyDescriptor.Builder()
        .required(true)
        .name("Attribute Strategy")
        .description("Determines which FlowFile attributes should be added to the bundle. If 'Keep All Unique Attributes' is selected, any "
            + "attribute on any FlowFile that gets bundled will be kept unless its value conflicts with the value from another FlowFile. "
            + "If 'Keep Only Common Attributes' is selected, only the attributes that exist on all FlowFiles in the bundle, with the same "
            + "value, will be preserved.")
        .allowableValues(ATTRIBUTE_STRATEGY_ALL_COMMON, ATTRIBUTE_STRATEGY_ALL_UNIQUE)
        .defaultValue(ATTRIBUTE_STRATEGY_ALL_COMMON.getValue())
        .build();


    public static AttributeStrategy strategyFor(ProcessContext context) {
        final String strategyName = context.getProperty(ATTRIBUTE_STRATEGY).getValue();
        if (ATTRIBUTE_STRATEGY_ALL_UNIQUE.getValue().equals(strategyName)) {
            return new KeepUniqueAttributeStrategy();
        }
        if (ATTRIBUTE_STRATEGY_ALL_COMMON.getValue().equals(strategyName)) {
            return new KeepCommonAttributeStrategy();
        }

        return null;
    }
}
