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

import com.bazaarvoice.jolt.JsonUtil;
import com.bazaarvoice.jolt.JsonUtilImpl;

public class JsonUtils {
    static private ThreadLocal<JsonUtilImpl> utilThreadLocal = new ThreadLocal<JsonUtilImpl>() {
        @Override
        protected JsonUtilImpl initialValue() {
            return new JsonUtilImpl();
        }
    };

    /**
     * The method returns a thread local implementation of {@link JsonUtil}.
     * We should always use this instance, instead of creating another one directly, when it is used
     * in multi-thread environment.
     * @return a {@link JsonUtil} instance
     */
    public static JsonUtilImpl getInstance() {
        return utilThreadLocal.get();
    }
}
