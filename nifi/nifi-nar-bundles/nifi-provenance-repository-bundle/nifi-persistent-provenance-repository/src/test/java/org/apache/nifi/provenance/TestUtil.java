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
package org.apache.nifi.provenance;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.flowfile.FlowFile;

public class TestUtil {
	public static FlowFile createFlowFile(final long id, final long fileSize, final Map<String, String> attributes) {
        final Map<String, String> attrCopy = new HashMap<>(attributes);

        return new FlowFile() {
            @Override
            public long getId() {
                return id;
            }

            @Override
            public long getEntryDate() {
                return System.currentTimeMillis();
            }

            @Override
            public Set<String> getLineageIdentifiers() {
                return new HashSet<String>();
            }

            @Override
            public long getLineageStartDate() {
                return System.currentTimeMillis();
            }

            @Override
            public Long getLastQueueDate() {
                return System.currentTimeMillis();
            }

            @Override
            public boolean isPenalized() {
                return false;
            }

            @Override
            public String getAttribute(final String s) {
                return attrCopy.get(s);
            }

            @Override
            public long getSize() {
                return fileSize;
            }

            @Override
            public Map<String, String> getAttributes() {
                return attrCopy;
            }

            @Override
            public int compareTo(final FlowFile o) {
                return 0;
            }
        };
    }
}
