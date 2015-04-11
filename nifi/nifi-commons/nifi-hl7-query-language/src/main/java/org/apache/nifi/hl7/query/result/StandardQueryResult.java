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
package org.apache.nifi.hl7.query.result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.hl7.query.QueryResult;
import org.apache.nifi.hl7.query.ResultHit;
import org.apache.nifi.hl7.query.Selection;

public class StandardQueryResult implements QueryResult {
	private final List<Selection> selections;
	private final Set<Map<String, Object>> hits;
	private final Iterator<Map<String, Object>> hitIterator;
	
	public StandardQueryResult(final List<Selection> selections, final Set<Map<String, Object>> hits) {
		this.selections = selections;
		this.hits = hits;
		
		hitIterator = hits.iterator();
	}
	
	@Override
	public boolean isMatch() {
		return !hits.isEmpty();
	}

	@Override
	public List<String> getLabels() {
		final List<String> labels = new ArrayList<>();
		for ( final Selection selection : selections ) {
			labels.add(selection.getName());
		}
		return labels;
	}

	@Override
	public int getHitCount() {
		return hits.size();
	}
	
	@Override
	public ResultHit nextHit() {
		if ( hitIterator.hasNext() ) {
			return new StandardResultHit(hitIterator.next());
		} else {
			return null;
		}
	}
	
}
