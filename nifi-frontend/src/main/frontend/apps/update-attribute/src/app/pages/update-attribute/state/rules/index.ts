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

export const rulesFeatureKey = 'rules';

export interface DeleteSuccessResponse {
    revision: number;
    id: string;
}

export interface Rule {
    id: string;
    name: string;
    comments: string;
    conditions: Condition[];
    actions: Action[];
}

export interface RuleEntity {
    clientId: string;
    revision: number;
    processorId: string;
    disconnectedNodeAcknowledged: boolean;
    rule: Rule;
}

export interface RulesEntity {
    clientId: string;
    revision: number;
    processorId: string;
    rules: Rule[];
}

export interface Action {
    id: string;
    attribute: string;
    value: string;
}

export interface Condition {
    id: string;
    expression: string;
}

export interface NewRule {
    name: string;
    comments: string;
    conditions: Condition[];
    actions: Action[];
}

export interface RulesState {
    loading: boolean;
    saving: boolean;
    error: string | null;
    newRule?: NewRule;
    rules: Rule[] | null;
}
