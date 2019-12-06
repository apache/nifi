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

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.rules.Action
import org.apache.nifi.rules.engine.RulesEngineService


class GroovyRulesEngine extends AbstractControllerService implements RulesEngineService {


    @Override
    List<Action> fireRules(Map<String, Object> facts) {
        def actions = [] as List<Action>
        if (facts['predictedQueuedCount'] > 50) {
            actions << new Action('LOG', ['level': 'DEBUG'])
        }
        if (facts['predictedTimeToBytesBackpressureMillis'] < 300000) {
            actions << new Action('ALERT', ['message': 'Time to backpressure < 5 mins'])
        }
        actions
    }
}

rulesEngine = new GroovyRulesEngine()
