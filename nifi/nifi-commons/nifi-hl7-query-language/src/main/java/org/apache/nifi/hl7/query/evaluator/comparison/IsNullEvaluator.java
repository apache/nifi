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
package org.apache.nifi.hl7.query.evaluator.comparison;

import java.util.Collection;
import java.util.Map;

import org.apache.nifi.hl7.model.HL7Component;
import org.apache.nifi.hl7.query.evaluator.BooleanEvaluator;
import org.apache.nifi.hl7.query.evaluator.Evaluator;

public class IsNullEvaluator extends BooleanEvaluator {
	private final Evaluator<?> subjectEvaluator;
	
	public IsNullEvaluator(final Evaluator<?> subjectEvaluator) {
		this.subjectEvaluator = subjectEvaluator;
	}
	
	@Override
	public Boolean evaluate(final Map<String, Object> objectMap) {
		Object subjectValue = subjectEvaluator.evaluate(objectMap);
		if ( subjectValue == null ) {
			return true;
		}
		
		return isNull(subjectValue);
	}

	private boolean isNull(Object subjectValue) {
		if ( subjectValue == null ) {
			return true;
		}
		
		if ( subjectValue instanceof HL7Component ) {
			subjectValue = ((HL7Component) subjectValue).getValue();
		}
		
		if ( subjectValue instanceof Collection ) {
			final Collection<?> collection = (Collection<?>) subjectValue;
			if ( collection.isEmpty() ) {
				return true;
			}
			
			for ( final Object obj : collection ) {
				if ( !isNull(obj) ) {
					return false;
				}
			}
			
			return true;
		}
		
		return subjectValue == null;
	}
}
