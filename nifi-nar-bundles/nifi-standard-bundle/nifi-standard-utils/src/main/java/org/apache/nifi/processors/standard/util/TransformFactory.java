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

package org.apache.nifi.processors.standard.util;

import com.bazaarvoice.jolt.CardinalityTransform;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Sortr;
import com.bazaarvoice.jolt.Transform;

public class TransformFactory {

    public static Transform getTransform(String transform, Object specJson) {
        if (transform.equals("jolt-transform-default")) {
            return new Defaultr(specJson);
        } else if (transform.equals("jolt-transform-shift")) {
            return new Shiftr(specJson);
        } else if (transform.equals("jolt-transform-remove")) {
            return new Removr(specJson);
        } else if (transform.equals("jolt-transform-card")) {
            return new CardinalityTransform(specJson);
        } else if(transform.equals("jolt-transform-sort")){
            return new Sortr();
        } else {
            return Chainr.fromSpec(specJson);
        }
    }

}
