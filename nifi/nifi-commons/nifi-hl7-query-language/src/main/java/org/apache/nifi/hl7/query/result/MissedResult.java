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
import java.util.List;

import org.apache.nifi.hl7.query.QueryResult;
import org.apache.nifi.hl7.query.ResultHit;
import org.apache.nifi.hl7.query.Selection;

public class MissedResult implements QueryResult {
	private final List<Selection> selections;
	
	public MissedResult(final List<Selection> selections) {
		this.selections = selections;
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
	public boolean isMatch() {
		return false;
	}

	@Override
	public ResultHit nextHit() {
		return null;
	}
	
	@Override
	public int getHitCount() {
		return 0;
	}
}
