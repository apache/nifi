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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.web.api.dto.util.TimeAdapter;

import com.wordnik.swagger.annotations.ApiModelProperty;


/**
 * All the counters in this NiFi instance at a given time.
 */
@XmlType(name = "countersSnapshot")
public class CountersSnapshotDTO implements Cloneable {

    private Date generated;
    private Collection<CounterDTO> counters;

    @ApiModelProperty("All counters in the NiFi.")
    public Collection<CounterDTO> getCounters() {
        return counters;
    }

    public void setCounters(Collection<CounterDTO> counters) {
        this.counters = counters;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp when the report was generated.",
            dataType = "string"
    )
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    @Override
    public CountersSnapshotDTO clone() {
        final CountersSnapshotDTO other = new CountersSnapshotDTO();
        other.setGenerated(getGenerated());

        final List<CounterDTO> clonedCounters = new ArrayList<>(getCounters().size());
        for (final CounterDTO counterDto : getCounters()) {
            clonedCounters.add(counterDto.clone());
        }

        other.setCounters(clonedCounters);
        return other;
    }
}
