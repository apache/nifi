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

package org.apache.nifi.minifi.bootstrap.configuration;

public class ListenerHandleResult {

    private final ConfigurationChangeListener configurationChangeListener;
    private final Exception failureCause;

    public ListenerHandleResult(ConfigurationChangeListener configurationChangeListener) {
        this.configurationChangeListener = configurationChangeListener;
        failureCause = null;
    }

    public ListenerHandleResult(ConfigurationChangeListener configurationChangeListener, Exception failureCause) {
        this.configurationChangeListener = configurationChangeListener;
        this.failureCause = failureCause;
    }

    public boolean succeeded() {
        return failureCause == null;
    }

    public String getDescriptor() {
        return configurationChangeListener.getDescriptor();
    }

    public Exception getFailureCause() {
        return failureCause;
    }

    @Override
    public String toString() {
        if (failureCause == null) {
            return getDescriptor() + " successfully handled the configuration change";
        } else {
            return getDescriptor() + " FAILED to handle the configuration change due to: '" + failureCause.getMessage() + "'";
        }
    }
}
