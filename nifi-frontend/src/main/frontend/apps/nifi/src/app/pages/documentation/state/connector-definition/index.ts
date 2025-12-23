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

import { ExtensionComponent, PropertyAllowableValue } from '../index';

export const connectorDefinitionFeatureKey = 'connectorDefinition';

export interface ConnectorPropertyDependency {
    propertyName: string;
    dependentValues?: string[];
}

export interface ConnectorPropertyDescriptor {
    name: string;
    description: string;
    defaultValue?: string;
    required: boolean;
    propertyType?: string;
    allowableValuesFetchable: boolean;
    allowableValues?: PropertyAllowableValue[];
    dependencies?: ConnectorPropertyDependency[];
}

export interface ConnectorPropertyGroup {
    name: string;
    description: string;
    properties?: ConnectorPropertyDescriptor[];
}

export interface ConfigurationStepDependency {
    stepName: string;
    propertyName: string;
    dependentValues?: string[];
}

export interface ConfigurationStep {
    name: string;
    description: string;
    documented: boolean;
    stepDependencies?: ConfigurationStepDependency[];
    propertyGroups?: ConnectorPropertyGroup[];
}

export interface ConnectorDefinition extends ExtensionComponent {
    configurationSteps?: ConfigurationStep[];
}

export interface StepDocumentationState {
    documentation: string | null;
    error: string | null;
    status: 'pending' | 'loading' | 'success' | 'error';
}

export interface ConnectorDefinitionState {
    connectorDefinition: ConnectorDefinition | null;
    error: string | null;
    status: 'pending' | 'loading' | 'success' | 'error';
    stepDocumentation: { [stepName: string]: StepDocumentationState };
}
