/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createAction, props } from '@ngrx/store';
import { DeleteSuccessResponse, NewRule, Rule, RuleEntity, RulesEntity } from './index';

const RULES_PREFIX = '[Rules]';

export const resetRulesState = createAction(`${RULES_PREFIX} Reset Rules State`);

export const loadRules = createAction(`${RULES_PREFIX} Load Rules`);

export const loadRulesSuccess = createAction(
    `${RULES_PREFIX} Load Rules Success`,
    props<{ rulesEntity: RulesEntity }>()
);

export const loadRulesFailure = createAction(`${RULES_PREFIX} Load Rules Failure`, props<{ error: string }>());

export const saveRuleFailure = createAction(`${RULES_PREFIX} Save Rule Failure`, props<{ error: string }>());

export const populateNewRule = createAction(
    `${RULES_PREFIX} Populate New Rule`,
    props<{
        rule?: Rule;
    }>()
);

export const resetNewRule = createAction(`${RULES_PREFIX} Reset New Rule`);

export const createRule = createAction(`${RULES_PREFIX} Create Rule`, props<{ newRule: NewRule }>());

export const createRuleSuccess = createAction(`${RULES_PREFIX} Create Rule Success`, props<{ entity: RuleEntity }>());

export const editRule = createAction(`${RULES_PREFIX} Edit Rule`, props<{ rule: Rule }>());

export const editRuleSuccess = createAction(`${RULES_PREFIX} Edit Rule Success`, props<{ entity: RuleEntity }>());

export const promptRuleDeletion = createAction(`${RULES_PREFIX} Prompt Rule Deletion`, props<{ rule: Rule }>());

export const deleteRule = createAction(`${RULES_PREFIX} Delete Rule`, props<{ rule: Rule }>());

export const deleteRuleSuccess = createAction(
    `${RULES_PREFIX} Delete Rule Success`,
    props<{ response: DeleteSuccessResponse }>()
);
