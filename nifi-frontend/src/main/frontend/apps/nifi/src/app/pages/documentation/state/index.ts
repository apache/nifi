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

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { processorDefinitionFeatureKey, ProcessorDefinitionState } from './processor-definition';
import { processorDefinitionReducer } from './processor-definition/processor-definition.reducer';
import {
    controllerServiceDefinitionFeatureKey,
    ControllerServiceDefinitionState
} from './controller-service-definition';
import { controllerServiceDefinitionReducer } from './controller-service-definition/controller-service-definition.reducer';
import { additionalDetailsFeatureKey, AdditionalDetailsState } from './additional-details';
import { additionalDetailsReducer } from './additional-details/additional-details.reducer';
import { externalDocumentationFeatureKey, ExternalDocumentationState } from './external-documentation';
import { externalDocumentationReducer } from './external-documentation/external-documentation.reducer';
import { reportingTaskDefinitionFeatureKey, ReportingTaskDefinitionState } from './reporting-task-definition';
import { reportingTaskDefinitionReducer } from './reporting-task-definition/reporting-task-definition.reducer';
import {
    parameterProviderDefinitionFeatureKey,
    ParameterProviderDefinitionState
} from './parameter-provider-definition';
import { parameterProviderDefinitionReducer } from './parameter-provider-definition/parameter-provider-definition.reducer';
import { flowAnalysisRuleDefinitionFeatureKey, FlowAnalysisRuleDefinitionState } from './flow-analysis-rule-definition';
import { flowAnalysisRuleDefinitionReducer } from './flow-analysis-rule-definition/flow-analysis-rule-definition.reducer';
import { ComponentType } from '@nifi/shared';
import { DocumentedType } from '../../../state/shared';

export const documentationFeatureKey = 'documentation';

export enum ExpressionLanguageScope {
    NONE = 'NONE',
    ENVIRONMENT = 'ENVIRONMENT',
    FLOWFILE_ATTRIBUTES = 'FLOWFILE_ATTRIBUTES'
}

export interface PropertyAllowableValue {
    value: string;
    displayName: string;
    description: string;
}

export interface PropertyDependency {
    propertyName: string;
    propertyDisplayName: string;
    dependentValues?: string[];
}

export enum ResourceCardinality {
    SINGLE = 'SINGLE',
    MULTIPLE = 'MULTIPLE'
}

export enum ResourceType {
    FILE = 'FILE',
    DIRECTORY = 'DIRECTORY',
    TEXT = 'TEXT',
    URL = 'URL'
}

export interface PropertyResourceDefinition {
    cardinality: ResourceCardinality;
    resourceTypes: ResourceType[];
}

export interface PropertyDescriptor {
    name: string;
    displayName: string;
    description: string;
    defaultValue?: string;
    allowableValues?: PropertyAllowableValue[];
    required: boolean;
    sensitive: boolean;
    dynamic: boolean;
    supportsEl: boolean;
    expressionLanguageScope: ExpressionLanguageScope;
    expressionLanguageScopeDescription: string;
    dependencies?: PropertyDependency[];
    typeProvidedByValue?: DefinedType;
    validRegex: string;
    validator: string;
    resourceDefinition?: PropertyResourceDefinition;
}

export interface DynamicProperty {
    name: string;
    value: string;
    description: string;
    expressionLanguageScope: ExpressionLanguageScope;
}

export interface ConfigurableExtensionDefinition extends ExtensionComponent {
    propertyDescriptors?: {
        [key: string]: PropertyDescriptor;
    };
    supportsDynamicProperties: boolean;
    supportsSensitiveDynamicProperties: boolean;
    dynamicProperties?: {
        [key: string]: DynamicProperty;
    };
}

export interface BuildInfo {
    revision: string;
    version?: string;
    timestamp?: number;
    targetArch?: string;
    compiler?: string;
    compilerFlags?: string;
}

export interface DefinedType {
    group: string;
    artifact: string;
    version: string;
    type: string;
    typeDescription: string;
}

export interface Restriction {
    requiredPermission: string;
    explanation: string;
}

export enum Scope {
    CLUSTER = 'CLUSTER',
    LOCAL = 'LOCAL'
}

export interface Stateful {
    description: string;
    scopes: Scope[];
}

export interface SystemResourceConsideration {
    resource: string;
    description: string;
}

export interface ExtensionComponent extends DefinedType {
    buildInfo: BuildInfo;
    providedApiImplementations?: DefinedType;
    tags: string[];
    seeAlso?: string[];
    deprecated?: boolean;
    deprecationReason?: string;
    deprecationAlternatives?: string[];
    restricted?: boolean;
    restrictedExplanation?: string;
    explicitRestrictions?: Restriction[];
    stateful?: Stateful;
    systemResourceConsiderations?: SystemResourceConsideration[];
    additionalDetails: boolean;
}

export interface DefinitionCoordinates {
    group: string;
    artifact: string;
    version: string;
    type: string;
}

export interface ExternalDocumentation {
    name: string;
    displayName: string;
    url: string;
}

export interface ComponentDocumentedType {
    componentType: ComponentType;
    documentedType: DocumentedType;
}

export interface DocumentationState {
    [processorDefinitionFeatureKey]: ProcessorDefinitionState;
    [controllerServiceDefinitionFeatureKey]: ControllerServiceDefinitionState;
    [reportingTaskDefinitionFeatureKey]: ReportingTaskDefinitionState;
    [parameterProviderDefinitionFeatureKey]: ParameterProviderDefinitionState;
    [flowAnalysisRuleDefinitionFeatureKey]: FlowAnalysisRuleDefinitionState;
    [additionalDetailsFeatureKey]: AdditionalDetailsState;
    [externalDocumentationFeatureKey]: ExternalDocumentationState;
}

export function reducers(state: DocumentationState | undefined, action: Action) {
    return combineReducers({
        [processorDefinitionFeatureKey]: processorDefinitionReducer,
        [controllerServiceDefinitionFeatureKey]: controllerServiceDefinitionReducer,
        [reportingTaskDefinitionFeatureKey]: reportingTaskDefinitionReducer,
        [parameterProviderDefinitionFeatureKey]: parameterProviderDefinitionReducer,
        [flowAnalysisRuleDefinitionFeatureKey]: flowAnalysisRuleDefinitionReducer,
        [additionalDetailsFeatureKey]: additionalDetailsReducer,
        [externalDocumentationFeatureKey]: externalDocumentationReducer
    })(state, action);
}

export const selectDocumentationState = createFeatureSelector<DocumentationState>(documentationFeatureKey);
