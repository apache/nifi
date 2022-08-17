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
package org.apache.nifi.processor.util.bin;

import java.util.HashMap;
import java.util.Map;

/**
 * Convenience class used to add attributes in the origin flow files once the bin was committed
 */
public class BinProcessingResult {

    /**
     * <code>true</code> if the processed bin was already committed. E.g., in case of a failure, the implementation
     * may choose to transfer all binned files to Failure and commit their sessions. If
     * false, the processBins() method will transfer the files to Original and commit the sessions
     */
    private boolean isCommitted;

    /**
     * Map of attributes to add to original flow files
     */
    private Map<String, String> attributes;

    public BinProcessingResult(boolean isCommitted) {
        this.setCommitted(isCommitted);
        this.setAttributes(new HashMap<String, String>());
    }

    public BinProcessingResult(boolean isCommitted, Map<String, String> attributes) {
        this.setCommitted(isCommitted);
        this.setAttributes(attributes);
    }

    public boolean isCommitted() {
        return isCommitted;
    }

    public void setCommitted(boolean isCommitted) {
        this.isCommitted = isCommitted;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

}
