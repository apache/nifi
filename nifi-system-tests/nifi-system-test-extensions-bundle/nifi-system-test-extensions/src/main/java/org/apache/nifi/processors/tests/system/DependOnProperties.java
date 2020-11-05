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

package org.apache.nifi.processors.tests.system;

import java.util.Arrays;
import java.util.List;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class DependOnProperties extends AbstractProcessor {

    static final AllowableValue FOO = new AllowableValue("foo");
    static final AllowableValue BAR = new AllowableValue("bar");
    static final AllowableValue BAZ = new AllowableValue("baz");

    static final PropertyDescriptor ALWAYS_REQUIRED = new Builder()
        .name("Always Required")
        .displayName("Always Required")
        .description("This property is always required")
        .required(true)
        .allowableValues(FOO, BAR, BAZ)
        .build();

    static final PropertyDescriptor ALWAYS_OPTIONAL = new Builder()
        .name("Always Optional")
        .displayName("Always Optional")
        .description("This property is always optional")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor REQUIRED_IF_OPTIONAL_PROPERTY_SET = new Builder()
        .name("Required If Optional Property Set")
        .displayName("Required If Optional Property Set")
        .description("This property is required if and only if the 'Always Optional' property is set (to anything)")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .dependsOn(ALWAYS_OPTIONAL)
        .build();

    static final PropertyDescriptor REQUIRED_IF_OPTIONAL_IS_FOO = new Builder()
        .name("Required If Optional Property Set To Foo")
        .displayName("Required If Optional Property Set To Foo")
        .description("This property is required if and only if the 'Always Optional' property is set to the value 'foo'")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .dependsOn(ALWAYS_OPTIONAL, "foo")
        .build();

    static final PropertyDescriptor REQUIRED_IF_ALWAYS_REQUIRED_IS_BAR_OR_BAZ = new Builder()
        .name("Required If Always Required Is Bar Or Baz")
        .displayName("Required If Always Required Is Bar Or Baz")
        .description("This property is required if and only if the 'Always Required' property is set to the value 'bar' or the value 'baz'")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .dependsOn(ALWAYS_REQUIRED, BAR, BAZ)
        .build();

    static final PropertyDescriptor SECOND_LEVEL_DEPENDENCY = new Builder()
        .name("Second Level Dependency")
        .displayName("Second Level Dependency")
        .description("Depends on 'Required If Optional Property Set To Foo'")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .dependsOn(REQUIRED_IF_OPTIONAL_IS_FOO)
        .build();

    static final PropertyDescriptor MULTIPLE_DEPENDENCIES = new Builder()
        .name("Multiple Dependencies")
        .displayName("Multiple Dependencies")
        .description("Depends on Always Required = foo AND Always Optional = bar")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .dependsOn(ALWAYS_REQUIRED, FOO)
        .dependsOn(ALWAYS_OPTIONAL, "bar")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
            ALWAYS_REQUIRED,
            ALWAYS_OPTIONAL,
            REQUIRED_IF_OPTIONAL_PROPERTY_SET,
            REQUIRED_IF_OPTIONAL_IS_FOO,
            REQUIRED_IF_ALWAYS_REQUIRED_IS_BAR_OR_BAZ,
            SECOND_LEVEL_DEPENDENCY,
            MULTIPLE_DEPENDENCIES);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    }
}
