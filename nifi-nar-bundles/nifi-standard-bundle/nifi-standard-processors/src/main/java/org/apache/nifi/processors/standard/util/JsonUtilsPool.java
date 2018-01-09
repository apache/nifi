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

import com.bazaarvoice.jolt.JsonUtil;
import com.bazaarvoice.jolt.JsonUtilImpl;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtilsPool {
    private final static Logger LOG = LoggerFactory.getLogger(JsonUtilsPool.class);
    static private GenericObjectPool pool;

    static {
        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
        config.timeBetweenEvictionRunsMillis = 60 * 1000; // 1 minute
        config.numTestsPerEvictionRun = 20;
        config.minEvictableIdleTimeMillis = 60 * 1000; // 1 minute
        config.minIdle = 1000;

        PoolableObjectFactory factory = new PoolableObjectFactory() {
            @Override
            public Object makeObject() throws Exception {
                return new JsonUtilImpl();
            }

            @Override
            public void destroyObject(Object o) throws Exception {}

            @Override
            public boolean validateObject(Object o) {
                return true;
            }

            @Override
            public void activateObject(Object o) throws Exception {}

            @Override
            public void passivateObject(Object o) throws Exception {}
        };
        pool = new GenericObjectPool(factory, config);
    }

    /**
     * Returns an implementation of {@link JsonUtil} from a pool.
     * Every instance got like this should be returned using {@link #returnInstance} method to the pool.
     * @return a {@link JsonUtil} instance
     */
    public static JsonUtil getInstance() {
        try {
            return (JsonUtilImpl) pool.borrowObject();
        } catch (Exception e) {
            // we should never be here, since we set whenExhaustedAction to WHEN_EXHAUSTED_GROW
            LOG.error("Exception while borrowing object to the pool", e);
            throw new RuntimeException(e);
        }
    }


    /**
     * Returns the object to the pool.
     */
    public static void returnInstance(JsonUtil o) {
        try {
            pool.returnObject(o);
        } catch (Exception e) {
            LOG.error("Exception while returning object to the pool", e);
        }
    }
}
