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

package org.apache.nifi.controller.swap;

import java.util.Collections;
import java.util.List;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapSummary;

public class StandardSwapContents implements SwapContents {
    public static final SwapContents EMPTY_SWAP_CONTENTS = new StandardSwapContents(StandardSwapSummary.EMPTY_SUMMARY, Collections.<FlowFileRecord> emptyList());

    private final SwapSummary summary;
    private final List<FlowFileRecord> flowFiles;

    public StandardSwapContents(final SwapSummary summary, final List<FlowFileRecord> flowFiles) {
        this.summary = summary;
        this.flowFiles = flowFiles;
    }

    @Override
    public SwapSummary getSummary() {
        return summary;
    }

    @Override
    public List<FlowFileRecord> getFlowFiles() {
        return flowFiles;
    }
}
