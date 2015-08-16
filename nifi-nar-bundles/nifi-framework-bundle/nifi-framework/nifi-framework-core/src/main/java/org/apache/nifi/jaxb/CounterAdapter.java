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
package org.apache.nifi.jaxb;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.StandardCounter;

/**
 */
public class CounterAdapter extends XmlAdapter<AdaptedCounter, Counter> {

    @Override
    public Counter unmarshal(final AdaptedCounter aCounter) throws Exception {
        if (aCounter == null) {
            return null;
        }
        final Counter counter = new StandardCounter(aCounter.getIdentifier(), aCounter.getContext(), aCounter.getName());
        counter.adjust(aCounter.getValue());
        return counter;
    }

    @Override
    public AdaptedCounter marshal(final Counter counter) throws Exception {
        if (counter == null) {
            return null;
        }
        final AdaptedCounter aCounter = new AdaptedCounter();
        aCounter.setIdentifier(counter.getIdentifier());
        aCounter.setValue(counter.getValue());
        aCounter.setContext(counter.getContext());
        aCounter.setName(counter.getName());
        return aCounter;
    }

}
