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
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { ProcessorDefinition } from '../state/processor-definition';
import { ControllerServiceDefinition } from '../state/controller-service-definition';
import { DefinitionCoordinates } from '../state';
import { AdditionalDetailsEntity } from '../state/additional-details';
import { ReportingTaskDefinition } from '../state/reporting-task-definition';
import { ParameterProviderDefinition } from '../state/parameter-provider-definition';
import { FlowAnalysisRuleDefinition } from '../state/flow-analysis-rule-definition';

@Injectable({ providedIn: 'root' })
export class DocumentationService {
    private static readonly API: string = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getProcessorDefinition(coordinates: DefinitionCoordinates): Observable<ProcessorDefinition> {
        return this.httpClient.get<ProcessorDefinition>(
            `${DocumentationService.API}/flow/processor-definition/${coordinates.group}/${coordinates.artifact}/${coordinates.version}/${coordinates.type}`
        );
    }

    getControllerServiceDefinition(coordinates: DefinitionCoordinates): Observable<ControllerServiceDefinition> {
        return this.httpClient.get<ControllerServiceDefinition>(
            `${DocumentationService.API}/flow/controller-service-definition/${coordinates.group}/${coordinates.artifact}/${coordinates.version}/${coordinates.type}`
        );
    }

    getReportingTaskDefinition(coordinates: DefinitionCoordinates): Observable<ReportingTaskDefinition> {
        return this.httpClient.get<ReportingTaskDefinition>(
            `${DocumentationService.API}/flow/reporting-task-definition/${coordinates.group}/${coordinates.artifact}/${coordinates.version}/${coordinates.type}`
        );
    }

    getParameterProviderDefinition(coordinates: DefinitionCoordinates): Observable<ParameterProviderDefinition> {
        return this.httpClient.get<ParameterProviderDefinition>(
            `${DocumentationService.API}/flow/parameter-provider-definition/${coordinates.group}/${coordinates.artifact}/${coordinates.version}/${coordinates.type}`
        );
    }

    getFlowAnalysisRuleDefinition(coordinates: DefinitionCoordinates): Observable<FlowAnalysisRuleDefinition> {
        return this.httpClient.get<FlowAnalysisRuleDefinition>(
            `${DocumentationService.API}/flow/flow-analysis-rule-definition/${coordinates.group}/${coordinates.artifact}/${coordinates.version}/${coordinates.type}`
        );
    }

    getAdditionalDetails(coordinates: DefinitionCoordinates): Observable<AdditionalDetailsEntity> {
        return this.httpClient.get<AdditionalDetailsEntity>(
            `${DocumentationService.API}/flow/additional-details/${coordinates.group}/${coordinates.artifact}/${coordinates.version}/${coordinates.type}`
        );
    }
}
