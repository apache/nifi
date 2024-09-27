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

import { ConfigurableExtensionDefinition } from '../index';

export const processorDefinitionFeatureKey = 'processorDefinition';

export enum InputRequirement {
    INPUT_REQUIRED = 'INPUT_REQUIRED',
    INPUT_ALLOWED = 'INPUT_ALLOWED',
    INPUT_FORBIDDEN = 'INPUT_FORBIDDEN'
}

export interface Relationship {
    description: string;
    name: string;
}

export interface Attribute {
    description: string;
    name: string;
}

export interface UseCase {
    description: string;
    notes?: string;
    keywords: string[];
    configuration: string;
    inputRequirement?: InputRequirement;
}

export interface ProcessorConfiguration {
    processorClassName: string;
    configuration: string;
}

export interface MultiProcessorUseCase {
    description: string;
    notes: string;
    keywords: string[];
    configurations: ProcessorConfiguration[];
}

export interface ProcessorDefinition extends ConfigurableExtensionDefinition {
    inputRequirement?: InputRequirement;
    supportedRelationships: Relationship[];
    supportsDynamicRelationships: boolean;
    dynamicRelationship?: Relationship;
    triggerSerially: boolean;
    triggerWhenEmpty: boolean;
    triggerWhenAnyDestinationAvailable: boolean;
    supportsBatching: boolean;
    primaryNodeOnly: boolean;
    sideEffectFree: boolean;
    supportedSchedulingStrategies: string[];
    defaultSchedulingStrategy: string;
    defaultConcurrentTasksBySchedulingStrategy: {
        [key: string]: number;
    };
    defaultSchedulingPeriodsBySchedulingStrategy: {
        [key: string]: string;
    };
    defaultPenaltyDuration: string;
    defaultYieldDuration: string;
    defaultBulletinLevel: string;
    readsAttributes?: Attribute[];
    writesAttributes?: Attribute[];
    useCases?: UseCase[];
    multiProcessorUseCases?: MultiProcessorUseCase[];
}

export interface ProcessorDefinitionState {
    processorDefinition: ProcessorDefinition | null;
    error: string | null;
    status: 'pending' | 'loading' | 'success' | 'error';
}
