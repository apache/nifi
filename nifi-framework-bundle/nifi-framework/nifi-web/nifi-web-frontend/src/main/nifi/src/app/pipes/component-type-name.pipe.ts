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

import { Pipe, PipeTransform } from '@angular/core';
import { ComponentType } from '../state/shared';

@Pipe({
    name: 'componentTypeName',
    standalone: true
})
export class ComponentTypeNamePipe implements PipeTransform {
    transform(type: ComponentType | string): string {
        switch (type) {
            case ComponentType.Connection:
                return 'Connection';
            case ComponentType.Processor:
                return 'Processor';
            case ComponentType.OutputPort:
                return 'Output Port';
            case ComponentType.InputPort:
                return 'Input Port';
            case ComponentType.ProcessGroup:
                return 'Process Group';
            case ComponentType.ControllerService:
                return 'Controller Service';
            case ComponentType.Flow:
                return 'Flow';
            case ComponentType.FlowAnalysisRule:
                return 'Flow Analysis Rule';
            case ComponentType.FlowRegistryClient:
                return 'Flow Registry Client';
            case ComponentType.Funnel:
                return 'Funnel';
            case ComponentType.Label:
                return 'Label';
            case ComponentType.ParameterProvider:
                return 'Parameter Provider';
            case ComponentType.RemoteProcessGroup:
                return 'Remote Process Group';
            case ComponentType.ReportingTask:
                return 'Reporting Task';
            default:
                return type;
        }
    }
}
