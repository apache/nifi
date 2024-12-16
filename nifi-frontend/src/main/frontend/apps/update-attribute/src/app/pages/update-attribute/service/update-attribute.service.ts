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

import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { EvaluationContext, EvaluationContextEntity } from '../state/evaluation-context';
import { AdvancedUiParameters } from '../state/advanced-ui-parameters';
import { NewRule, Rule, RuleEntity, RulesEntity } from '../state/rules';

@Injectable({ providedIn: 'root' })
export class UpdateAttributeService {
    private static readonly API: string = 'api';

    constructor(private httpClient: HttpClient) {}

    getEvaluationContext(processorId: string): Observable<EvaluationContextEntity> {
        const params = new HttpParams({
            fromObject: {
                processorId
            }
        });

        return this.httpClient.get<EvaluationContextEntity>(
            `${UpdateAttributeService.API}/criteria/evaluation-context`,
            { params }
        );
    }

    saveEvaluationContext(
        advancedUiParameters: AdvancedUiParameters,
        evaluationContext: EvaluationContext
    ): Observable<EvaluationContextEntity> {
        const entity = {
            revision: advancedUiParameters.revision,
            disconnectedNodeAcknowledged: advancedUiParameters.disconnectedNodeAcknowledged,
            clientId: advancedUiParameters.clientId,
            processorId: advancedUiParameters.processorId,
            ...evaluationContext
        } as EvaluationContextEntity;

        return this.httpClient.put<EvaluationContextEntity>(
            `${UpdateAttributeService.API}/criteria/evaluation-context`,
            entity
        );
    }

    getRules(processorId: string): Observable<RulesEntity> {
        const params = new HttpParams({
            fromObject: {
                processorId,
                verbose: true
            }
        });

        return this.httpClient.get<RulesEntity>(`${UpdateAttributeService.API}/criteria/rules`, { params });
    }

    createRule(advancedUiParameters: AdvancedUiParameters, newRule: NewRule): Observable<RuleEntity> {
        const entity = {
            revision: advancedUiParameters.revision,
            disconnectedNodeAcknowledged: advancedUiParameters.disconnectedNodeAcknowledged,
            clientId: advancedUiParameters.clientId,
            processorId: advancedUiParameters.processorId,
            rule: {
                ...newRule
            }
        } as RuleEntity;

        return this.httpClient.post<RuleEntity>(`${UpdateAttributeService.API}/criteria/rules`, entity);
    }

    saveRule(advancedUiParameters: AdvancedUiParameters, rule: Rule): Observable<RuleEntity> {
        const entity = {
            revision: advancedUiParameters.revision,
            disconnectedNodeAcknowledged: advancedUiParameters.disconnectedNodeAcknowledged,
            clientId: advancedUiParameters.clientId,
            processorId: advancedUiParameters.processorId,
            rule
        } as RuleEntity;

        return this.httpClient.put<RuleEntity>(`${UpdateAttributeService.API}/criteria/rules/${rule.id}`, entity);
    }

    deleteRule(advancedUiParameters: AdvancedUiParameters, ruleId: string): Observable<RuleEntity> {
        const params = new HttpParams({
            fromObject: {
                revision: advancedUiParameters.revision,
                disconnectedNodeAcknowledged: advancedUiParameters.disconnectedNodeAcknowledged,
                clientId: advancedUiParameters.clientId,
                processorId: advancedUiParameters.processorId
            }
        });

        return this.httpClient.delete<RuleEntity>(`${UpdateAttributeService.API}/criteria/rules/${ruleId}`, {
            params
        });
    }

    getErrorString(errorResponse: HttpErrorResponse): string {
        let errorMessage = 'An unspecified error occurred.';
        if (errorResponse.status !== 0) {
            if (errorResponse.error) {
                errorMessage = errorResponse.error;
            } else {
                errorMessage = errorResponse.message || `${errorResponse.status}`;
            }
        }
        return errorMessage;
    }
}
