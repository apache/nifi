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
package org.apache.nifi.hl7.query.evaluator.logic;

import java.util.Map;

import org.apache.nifi.hl7.query.evaluator.BooleanEvaluator;

public class AndEvaluator extends BooleanEvaluator {
	private final BooleanEvaluator lhs;
	private final BooleanEvaluator rhs;
	
	public AndEvaluator(final BooleanEvaluator lhs, final BooleanEvaluator rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}
	
	@Override
	public Boolean evaluate(final Map<String, Object> objectMap) {
		final Boolean lhsValue = lhs.evaluate(objectMap);
		if ( lhsValue == null || Boolean.FALSE.equals(lhsValue) ) {
			return false;
		}
		
		final Boolean rhsValue = rhs.evaluate(objectMap);
		return (rhsValue != null && Boolean.TRUE.equals(rhsValue));
	}

}
