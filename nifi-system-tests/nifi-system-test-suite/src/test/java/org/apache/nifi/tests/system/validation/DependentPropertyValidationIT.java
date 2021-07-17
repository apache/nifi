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

package org.apache.nifi.tests.system.validation;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DependentPropertyValidationIT extends NiFiSystemIT {

    @Test(timeout = 20_000)
    public void testPropertyDependenciesAreValidatedProperly() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("DependOnProperties");
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Required", "foo"));

        getClientUtil().waitForValidProcessor(processor.getId());

        // Processor will become invalid because "Always Optional" is set and "Required If Optional Property Set" is not.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Optional", "hello"));
        getClientUtil().waitForInvalidProcessor(processor.getId());

        // Set the "Required If Optional Property Set" property. This should make the processor valid.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Required If Optional Property Set", "hello"));
        getClientUtil().waitForValidProcessor(processor.getId());

        // If 'Always Optional' is set to 'foo', then the 'Required If Optional Property Set To Foo' property must be set.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Optional", "foo"));
        getClientUtil().waitForInvalidProcessor(processor.getId());

        // Setting the 'Required If Optional Property Set To Foo' to any value will now make the processor valid.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Required If Optional Property Set To Foo", "hello"));
        getClientUtil().waitForValidProcessor(processor.getId());

        // Setting the 'Second Level Dependency' property to -42 will make the processor invalid because the property required a positive integer.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Second Level Dependency", "-42"));
        getClientUtil().waitForInvalidProcessor(processor.getId());

        // If We now set the "Always Optional" property to 'other', then the 'Second Level Dependency' property should not be validated.
        // This is because its dependency is not fully satisfied because of the 'transitive' dependency. I.e., it depends on 'Required If Optional Property Set To Foo'
        // but since 'Required If Optional Property Set To Foo' is not available/validated (because Always Optional is set to 'other'), then 'Second Level Dependency' is also not available/validated.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Optional", "other"));
        getClientUtil().waitForValidProcessor(processor.getId());

        // Setting the 'Always Required' property to 'bar' will result in requiring the 'Required If Always Required Is Bar Or Baz' property to be set.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Required", "bar"));
        getClientUtil().waitForInvalidProcessor(processor.getId());

        // Setting the 'Required If Always Required Is Bar Or Baz' to any positive integer should now make the processor valid.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Required If Always Required Is Bar Or Baz", "42"));
        getClientUtil().waitForValidProcessor(processor.getId());

        // Setting the property to -42 should result in an invalid processor because the property must be a positive integer.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Required If Always Required Is Bar Or Baz", "-42"));
        getClientUtil().waitForInvalidProcessor(processor.getId());

        // Changing the 'Always Required' back to 'foo' should result in the processor becoming valid again. Even though 'Required If Always Required Is Bar Or Baz' is invalid,
        // the processor is still valid because this property will no longer be considered.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Required", "foo"));
        getClientUtil().waitForValidProcessor(processor.getId());

        // Setting Always Required = foo and Always Optional = bar means that "Multiple Dependencies" will be required.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Always Optional", "bar"));
        getClientUtil().waitForInvalidProcessor(processor.getId());

        // Setting the "Multiple Dependencies" property to anything other than empty string will result in the processor becoming valid.
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Multiple Dependencies", "bar"));
        getClientUtil().waitForValidProcessor(processor.getId());

        // Processor should stay valid if we make Multiple Dependencies empty string (invalid) but also change Always Optional to "other" because that will result in "Multiple Dependencies"
        // no longer being available/validated.
        final Map<String, String> props = new HashMap<>();
        props.put("Multiple Dependencies", "");
        props.put("Always Optional", "other");
        getClientUtil().updateProcessorProperties(processor, props);
        getClientUtil().waitForValidProcessor(processor.getId());
    }

}
