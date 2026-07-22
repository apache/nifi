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

import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { defer, Observable } from 'rxjs';
import { safeApiPath } from '@nifi/shared';
import { ProcessorDefinition } from '../state/processor-definition';
import { ControllerServiceDefinition } from '../state/controller-service-definition';
import { DefinitionCoordinates } from '../state';
import { AdditionalDetailsEntity } from '../state/additional-details';
import { ReportingTaskDefinition } from '../state/reporting-task-definition';
import { ParameterProviderDefinition } from '../state/parameter-provider-definition';
import { FlowAnalysisRuleDefinition } from '../state/flow-analysis-rule-definition';
import { FlowRegistryClientDefinition } from '../state/flow-registry-client-definition';
import { ConnectorDefinition } from '../state/connector-definition';

@Injectable({ providedIn: 'root' })
export class DocumentationService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-api';

    /**
     * Build the validated/encoded `{group}/{artifact}/{version}/{type}` path suffix from
     * route-derived definition coordinates. Each atom is validated and encoded by safeApiPath,
     * which throws on a path separator or traversal sequence so untrusted deep-link input cannot
     * redirect the authenticated request to an arbitrary same-origin path.
     */
    private static coordinatePath(coordinates: DefinitionCoordinates): string {
        return safeApiPath(coordinates.group, coordinates.artifact, coordinates.version, coordinates.type);
    }

    getProcessorDefinition(coordinates: DefinitionCoordinates): Observable<ProcessorDefinition> {
        // Deferred so a safeApiPath rejection of untrusted (route-derived) input surfaces as an
        // observable error the caller's catchError can handle, rather than throwing synchronously.
        return defer(() =>
            this.httpClient.get<ProcessorDefinition>(
                `${DocumentationService.API}/flow/processor-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getControllerServiceDefinition(coordinates: DefinitionCoordinates): Observable<ControllerServiceDefinition> {
        return defer(() =>
            this.httpClient.get<ControllerServiceDefinition>(
                `${DocumentationService.API}/flow/controller-service-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getReportingTaskDefinition(coordinates: DefinitionCoordinates): Observable<ReportingTaskDefinition> {
        return defer(() =>
            this.httpClient.get<ReportingTaskDefinition>(
                `${DocumentationService.API}/flow/reporting-task-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getFlowRegistryClientDefinition(coordinates: DefinitionCoordinates): Observable<FlowRegistryClientDefinition> {
        return defer(() =>
            this.httpClient.get<FlowRegistryClientDefinition>(
                `${DocumentationService.API}/flow/flow-registry-client-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getParameterProviderDefinition(coordinates: DefinitionCoordinates): Observable<ParameterProviderDefinition> {
        return defer(() =>
            this.httpClient.get<ParameterProviderDefinition>(
                `${DocumentationService.API}/flow/parameter-provider-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getFlowAnalysisRuleDefinition(coordinates: DefinitionCoordinates): Observable<FlowAnalysisRuleDefinition> {
        return defer(() =>
            this.httpClient.get<FlowAnalysisRuleDefinition>(
                `${DocumentationService.API}/flow/flow-analysis-rule-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getAdditionalDetails(coordinates: DefinitionCoordinates): Observable<AdditionalDetailsEntity> {
        return defer(() =>
            this.httpClient.get<AdditionalDetailsEntity>(
                `${DocumentationService.API}/flow/additional-details/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getConnectorDefinition(coordinates: DefinitionCoordinates): Observable<ConnectorDefinition> {
        return defer(() =>
            this.httpClient.get<ConnectorDefinition>(
                `${DocumentationService.API}/flow/connector-definition/${DocumentationService.coordinatePath(coordinates)}`
            )
        );
    }

    getStepDocumentation(coordinates: DefinitionCoordinates, stepName: string): Observable<StepDocumentationEntity> {
        return defer(() =>
            this.httpClient.get<StepDocumentationEntity>(
                `${DocumentationService.API}/flow/steps/${DocumentationService.coordinatePath(coordinates)}/${safeApiPath(stepName)}`
            )
        );
    }
}

export interface StepDocumentationEntity {
    stepDocumentation: string;
}
