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
package org.apache.nifi.web.api.dto;

import java.util.Collection;
import java.util.Date;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * All the counters in this NiFi instance at a given time.
 */
@XmlType(name = "counters")
public class CountersDTO {

    private Date generated;
    private Collection<CounterDTO> counters;

    /**
     * @return the collection of counters
     */
    public Collection<CounterDTO> getCounters() {
        return counters;
    }

    public void setCounters(Collection<CounterDTO> counters) {
        this.counters = counters;
    }

    /**
     * @return the date/time that this report was generated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }
}
