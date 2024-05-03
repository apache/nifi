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

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';
import {
    ConfigureFlowAnalysisRuleRequest,
    CreateFlowAnalysisRuleRequest,
    DeleteFlowAnalysisRuleRequest,
    EnableFlowAnalysisRuleRequest,
    FlowAnalysisRuleEntity
} from '../state/flow-analysis-rules';
import { PropertyDescriptorRetriever } from '../../../state/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class FlowAnalysisRuleService implements PropertyDescriptorRetriever {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon,
        private clusterConnectionService: ClusterConnectionService
    ) {}

    getFlowAnalysisRule(): Observable<any> {
        return this.httpClient.get(`${FlowAnalysisRuleService.API}/controller/flow-analysis-rules`);
    }

    createFlowAnalysisRule(createFlowAnalysisRule: CreateFlowAnalysisRuleRequest): Observable<any> {
        return this.httpClient.post(`${FlowAnalysisRuleService.API}/controller/flow-analysis-rules`, {
            revision: createFlowAnalysisRule.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                bundle: createFlowAnalysisRule.flowAnalysisRuleBundle,
                type: createFlowAnalysisRule.flowAnalysisRuleType
            }
        });
    }

    deleteFlowAnalysisRule(deleteFlowAnalysisRule: DeleteFlowAnalysisRuleRequest): Observable<any> {
        const entity: FlowAnalysisRuleEntity = deleteFlowAnalysisRule.flowAnalysisRule;
        const params = new HttpParams({
            fromObject: {
                ...this.client.getRevision(entity),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        });
        return this.httpClient.delete(this.nifiCommon.stripProtocol(entity.uri), { params });
    }

    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<any> {
        const params: any = {
            propertyName,
            sensitive
        };
        return this.httpClient.get(`${FlowAnalysisRuleService.API}/controller/flow-analysis-rules/${id}/descriptors`, {
            params
        });
    }

    updateFlowAnalysisRule(configureFlowAnalysisRule: ConfigureFlowAnalysisRuleRequest): Observable<any> {
        return this.httpClient.put(
            this.nifiCommon.stripProtocol(configureFlowAnalysisRule.uri),
            configureFlowAnalysisRule.payload
        );
    }

    setEnable(flowAnalysisRule: EnableFlowAnalysisRuleRequest, enabled: boolean): Observable<any> {
        const entity: FlowAnalysisRuleEntity = flowAnalysisRule.flowAnalysisRule;
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(entity.uri)}/run-status`, {
            revision: this.client.getRevision(entity),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            state: enabled ? 'ENABLED' : 'DISABLED',
            uiOnly: true
        });
    }
}
