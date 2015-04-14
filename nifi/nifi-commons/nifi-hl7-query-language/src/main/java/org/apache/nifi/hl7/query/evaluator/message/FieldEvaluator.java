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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hl7.model.HL7Field;
import org.apache.nifi.hl7.model.HL7Segment;
import org.apache.nifi.hl7.query.evaluator.Evaluator;
import org.apache.nifi.hl7.query.evaluator.IntegerEvaluator;

@SuppressWarnings("rawtypes")
public class FieldEvaluator implements Evaluator<List> {
	private final SegmentEvaluator segmentEvaluator;
	private final IntegerEvaluator indexEvaluator;
	
	public FieldEvaluator(final SegmentEvaluator segmentEvaluator, final IntegerEvaluator indexEvaluator) {
		this.segmentEvaluator = segmentEvaluator;
		this.indexEvaluator = indexEvaluator;
	}
	
	public List<HL7Field> evaluate(final Map<String, Object> objectMap) {
		final List<HL7Segment> segments = segmentEvaluator.evaluate(objectMap);
		if ( segments == null ) {
			return Collections.emptyList();
		}
		
		final Integer index = indexEvaluator.evaluate(objectMap);
		if ( index == null ) {
			return Collections.emptyList();
		}
		
		final List<HL7Field> fields = new ArrayList<>();
		for ( final HL7Segment segment : segments ) {
			final List<HL7Field> segmentFields = segment.getFields();
			if ( segmentFields.size() <= index ) {
				continue;
			}
			
			fields.add(segmentFields.get(index));
		}
		
		return fields;
	}

	public Class<? extends List> getType() {
		return List.class;
	}

}
