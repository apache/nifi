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
package org.apache.nifi.processors.standard.util.jolt;

import java.util.Collections;
import java.util.Map;

import com.bazaarvoice.jolt.ContextualTransform;
import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.Transform;

public class TransformUtils {

    public static Object transform(JoltTransform joltTransform, Object input) {
        return joltTransform instanceof ContextualTransform
                ? ((ContextualTransform)joltTransform).transform(input, Collections.emptyMap()) : ((Transform) joltTransform).transform(input);
    }

    public static Object transform(JoltTransform joltTransform, Object input, Map<String,Object> contextMap) {
        return joltTransform instanceof ContextualTransform
                ? ((ContextualTransform)joltTransform).transform(input, contextMap) : ((Transform) joltTransform).transform(input);
    }

}
