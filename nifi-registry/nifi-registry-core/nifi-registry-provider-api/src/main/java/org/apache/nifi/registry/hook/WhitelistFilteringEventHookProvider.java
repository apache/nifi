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
package org.apache.nifi.registry.hook;

import org.apache.nifi.registry.provider.ProviderConfigurationContext;
import org.apache.nifi.registry.provider.ProviderCreationException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class WhitelistFilteringEventHookProvider
        implements EventHookProvider {

    static final String EVENT_WHITELIST_PREFIX = "Whitelisted Event Type ";
    static final Pattern EVENT_WHITELIST_PATTERN = Pattern.compile(EVENT_WHITELIST_PREFIX + "\\S+");

    protected Set<EventType> whiteListEvents = null;

    @Override
    public void onConfigured(ProviderConfigurationContext configurationContext) throws ProviderCreationException {
        whiteListEvents = new HashSet<>();
        for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
            Matcher matcher = EVENT_WHITELIST_PATTERN.matcher(entry.getKey());
            if (matcher.matches() && (entry.getValue() != null && entry.getValue().length() > 0)) {
                whiteListEvents.add(EventType.valueOf(entry.getValue()));
            }

        }
    }

    /**
     * Standard method for deciding if the EventType should be handled by the Hook provider or not.
     *
     * @param eventType
     *  EventType that was fired by the framework.
     *
     * @return
     *  True if the EventType is in the whitelist set and false otherwise.
     */
    @Override
    public boolean shouldHandle(EventType eventType) {
        if (whiteListEvents != null && whiteListEvents.size() > 0) {
            if (whiteListEvents.contains(eventType)) {
                return true;
            }
        } else {
            // If the whitelist property is not set or empty we want to fire for all events.
            return true;
        }
        return false;
    }

}
