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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SWAP_PROPS_KEY;

/**
 *
 */
public class SwapSchema extends BaseSchema {
    public static final String THRESHOLD_KEY = "threshold";
    public static final String IN_PERIOD_KEY = "in period";
    public static final String IN_THREADS_KEY = "in threads";
    public static final String OUT_PERIOD_KEY = "out period";
    public static final String OUT_THREADS_KEY = "out threads";

    public static final int DEFAULT_THRESHOLD = 20000;
    public static final String DEFAULT_IN_PERIOD = "5 sec";
    public static final int DEFAULT_IN_THREADS = 1;
    public static final String DEFAULT_OUT_PERIOD = "5 sec";
    public static final int DEFAULT_OUT_THREADS = 4;

    private Number threshold = DEFAULT_THRESHOLD;
    private String inPeriod = DEFAULT_IN_PERIOD;
    private Number inThreads = DEFAULT_IN_THREADS;
    private String outPeriod = DEFAULT_OUT_PERIOD;
    private Number outThreads = DEFAULT_OUT_THREADS;

    public SwapSchema() {
    }

    public SwapSchema(Map map) {
        threshold = getOptionalKeyAsType(map, THRESHOLD_KEY, Number.class, SWAP_PROPS_KEY, DEFAULT_THRESHOLD);
        inPeriod = getOptionalKeyAsType(map, IN_PERIOD_KEY, String.class, SWAP_PROPS_KEY, DEFAULT_IN_PERIOD);
        inThreads = getOptionalKeyAsType(map, IN_THREADS_KEY, Number.class, SWAP_PROPS_KEY, DEFAULT_IN_THREADS);
        outPeriod = getOptionalKeyAsType(map, OUT_PERIOD_KEY, String.class, SWAP_PROPS_KEY, DEFAULT_OUT_PERIOD);
        outThreads = getOptionalKeyAsType(map, OUT_THREADS_KEY, Number.class, SWAP_PROPS_KEY, DEFAULT_OUT_THREADS);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(THRESHOLD_KEY, threshold);
        result.put(IN_PERIOD_KEY, inPeriod);
        result.put(IN_THREADS_KEY, inThreads);
        result.put(OUT_PERIOD_KEY, outPeriod);
        result.put(OUT_THREADS_KEY, outThreads);
        return result;
    }

    public Number getThreshold() {
        return threshold;
    }

    public String getInPeriod() {
        return inPeriod;
    }

    public Number getInThreads() {
        return inThreads;
    }

    public String getOutPeriod() {
        return outPeriod;
    }

    public Number getOutThreads() {
        return outThreads;
    }
}
