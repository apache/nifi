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
package org.apache.nifi.hl7.hapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.hl7.model.HL7Field;
import org.apache.nifi.hl7.model.HL7Segment;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.model.Type;

public class HapiSegment implements HL7Segment {

    private final Segment segment;
    private final List<HL7Field> fields;

    public HapiSegment(final Segment segment) throws HL7Exception {
        this.segment = segment;

        final List<HL7Field> fieldList = new ArrayList<>();
        for (int i = 1; i <= segment.numFields(); i++) {
            final Type[] types = segment.getField(i);

            if (types == null || types.length == 0) {
                fieldList.add(new EmptyField());
                continue;
            }

            for (final Type type : types) {
                fieldList.add(new HapiField(type));
            }
        }

        this.fields = Collections.unmodifiableList(fieldList);
    }

    @Override
    public String getName() {
        return segment.getName();
    }

    @Override
    public List<HL7Field> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return segment.toString();
    }
}
