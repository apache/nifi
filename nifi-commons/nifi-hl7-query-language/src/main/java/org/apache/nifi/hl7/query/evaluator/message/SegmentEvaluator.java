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
package org.apache.nifi.hl7.query.evaluator.message;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hl7.model.HL7Message;
import org.apache.nifi.hl7.model.HL7Segment;
import org.apache.nifi.hl7.query.evaluator.Evaluator;
import org.apache.nifi.hl7.query.evaluator.StringEvaluator;

@SuppressWarnings("rawtypes")
public class SegmentEvaluator implements Evaluator<List> {

    private final StringEvaluator segmentTypeEvaluator;

    public SegmentEvaluator(final StringEvaluator segmentTypeEvaluator) {
        this.segmentTypeEvaluator = segmentTypeEvaluator;
    }

    public List<HL7Segment> evaluate(final Map<String, Object> objectMap) {
        final String segmentType = segmentTypeEvaluator.evaluate(objectMap);
        if (segmentType == null) {
            return Collections.emptyList();
        }

        final HL7Message message = (HL7Message) objectMap.get(Evaluator.MESSAGE_KEY);
        final List<HL7Segment> segments = message.getSegments(segmentType);
        return (segments == null) ? Collections.<HL7Segment>emptyList() : segments;
    }

    public Class<? extends List> getType() {
        return List.class;
    }

}
