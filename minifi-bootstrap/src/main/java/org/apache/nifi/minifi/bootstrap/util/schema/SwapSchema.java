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
package org.apache.nifi.minifi.bootstrap.util.schema;

import org.apache.nifi.minifi.bootstrap.util.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.SWAP_PROPS_KEY;

/**
 *
 */
public class SwapSchema extends BaseSchema {
    public static final String THRESHOLD_KEY = "threshold";
    public static final String IN_PERIOD_KEY = "in period";
    public static final String IN_THREADS_KEY = "in threads";
    public static final String OUT_PERIOD_KEY = "out period";
    public static final String OUT_THREADS_KEY = "out threads";

    private Number threshold = 20000;
    private String inPeriod = "5 sec";
    private Number inThreads = 1;
    private String outPeriod = "5 sec";
    private Number outThreads = 4;

    public SwapSchema() {
    }

    public SwapSchema(Map map) {
        threshold = getOptionalKeyAsType(map, THRESHOLD_KEY, Number.class, SWAP_PROPS_KEY, 20000);

        inPeriod = getOptionalKeyAsType(map, IN_PERIOD_KEY, String.class, SWAP_PROPS_KEY, "5 sec");

        inThreads = getOptionalKeyAsType(map, IN_THREADS_KEY, Number.class, SWAP_PROPS_KEY, 1);

        outPeriod = getOptionalKeyAsType(map, OUT_PERIOD_KEY, String.class, SWAP_PROPS_KEY, "5 sec");

        outThreads = getOptionalKeyAsType(map, OUT_THREADS_KEY, Number.class, SWAP_PROPS_KEY, 4);
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
