/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License') you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.context.PropertyContext
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.reporting.BulletinRepository
import org.apache.nifi.reporting.ReportingContext
import org.apache.nifi.reporting.Severity
import org.apache.nifi.rules.Action
import org.apache.nifi.rules.PropertyContextActionHandler


class GroovyPropertyContextActionHandler extends AbstractControllerService implements PropertyContextActionHandler {

    @Override
    void execute(Action action, Map<String, Object> facts) {
        // Add a fact for verification that execute was successfully performed
        facts['testFact'] = 42
    }

    @Override
    void execute(PropertyContext propertyContext, Action action, Map<String, Object> facts) {
        // Add a fact for verification that execute was successfully performed
        if (propertyContext instanceof ReportingContext) {
            ReportingContext context = (ReportingContext) propertyContext
            BulletinRepository bulletinRepository = context.bulletinRepository
            bulletinRepository.addBulletin(context.createBulletin('Rules Alert', Severity.INFO, 'This should be sent as an alert!\n'
                    + 'Alert Facts:\n'
                    + 'Field: cpu, Value: 90\n'
                    + 'Field: jvmHeap, Value: 1000000\n'))
        }
    }
}

actionHandler = new GroovyPropertyContextActionHandler()
