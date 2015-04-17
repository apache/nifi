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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hl7.model.HL7Message;
import org.apache.nifi.hl7.model.HL7Segment;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Group;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.model.Structure;

public class HapiMessage implements HL7Message {
	private final Message message;
	private final List<HL7Segment> allSegments;
	private final Map<String, List<HL7Segment>> segmentMap;
	
	public HapiMessage(final Message message) throws HL7Exception {
		this.message = message;
		
		allSegments = new ArrayList<>();
		populateSegments(message, allSegments);
		
		segmentMap = new HashMap<>();
		for ( final HL7Segment segment : allSegments ) {
			final String segmentName = segment.getName();
			List<HL7Segment> segmentList = segmentMap.get(segmentName);
			if ( segmentList == null ) {
				segmentList = new ArrayList<>();
				segmentMap.put(segmentName, segmentList);
			}
			
			segmentList.add(segment);
		}
	}
	
	private void populateSegments(final Group group, final List<HL7Segment> segments) throws HL7Exception {
		for ( final String structureName : group.getNames() ) {
			final Structure[] structures = group.getAll(structureName);
			if ( group.isGroup(structureName) ) {
				for ( final Structure structure : structures ) {
					populateSegments((Group) structure, segments);
				}
			} else {
				for ( final Structure structure : structures ) {
					final Segment segment = (Segment) structure;
					final HapiSegment hapiSegment = new HapiSegment(segment);
					segments.add(hapiSegment);
				}
			}
		}
	}
	
	@Override
	public List<HL7Segment> getSegments() {
		return Collections.unmodifiableList(allSegments);
	}

	@Override
	public List<HL7Segment> getSegments(final String segmentType) {
		final List<HL7Segment> segments = segmentMap.get(segmentType);
		if ( segments == null ) {
			return Collections.emptyList();
		}
		
		return Collections.unmodifiableList(segments);
	}

	@Override
	public String toString() {
		return message.toString();
	}
}
