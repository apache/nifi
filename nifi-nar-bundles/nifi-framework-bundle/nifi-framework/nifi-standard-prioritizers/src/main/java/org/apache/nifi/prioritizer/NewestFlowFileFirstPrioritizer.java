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
package org.apache.nifi.prioritizer;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;

public class NewestFlowFileFirstPrioritizer implements FlowFilePrioritizer {

    @Override
    public int compare(final FlowFile o1, final FlowFile o2) {
        if (o1 == null && o2 == null) {
            return 0;
        } else if (o2 == null) {
            return -1;
        } else if (o1 == null) {
            return 1;
        }

        final int lineageDateCompare = Long.compare(o2.getLineageStartDate(), o1.getLineageStartDate());
        if (lineageDateCompare != 0) {
            return lineageDateCompare;
        }

        return Long.compare(o2.getLineageStartIndex(), o1.getLineageStartIndex());
    }

}
