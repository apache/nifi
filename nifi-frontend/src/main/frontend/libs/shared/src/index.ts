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

import { MatDialogConfig } from '@angular/material/dialog';
import { filter, Observable } from 'rxjs';

export const SMALL_DIALOG: MatDialogConfig = {
    maxWidth: '24rem',
    minWidth: 320,
    disableClose: true,
    closeOnNavigation: false
};
export const MEDIUM_DIALOG: MatDialogConfig = {
    maxWidth: 470,
    minWidth: 470,
    disableClose: true,
    closeOnNavigation: false
};
export const LARGE_DIALOG: MatDialogConfig = {
    maxWidth: 760,
    minWidth: 760,
    disableClose: true,
    closeOnNavigation: false
};
export const XL_DIALOG: MatDialogConfig = {
    maxWidth: 1024,
    minWidth: 1024,
    disableClose: true,
    closeOnNavigation: false
};

export enum ComponentType {
    Processor = 'Processor',
    ProcessGroup = 'ProcessGroup',
    RemoteProcessGroup = 'RemoteProcessGroup',
    InputPort = 'InputPort',
    OutputPort = 'OutputPort',
    Label = 'Label',
    Funnel = 'Funnel',
    Connection = 'Connection',
    ControllerService = 'ControllerService',
    ReportingTask = 'ReportingTask',
    FlowAnalysisRule = 'FlowAnalysisRule',
    ParameterProvider = 'ParameterProvider',
    FlowRegistryClient = 'FlowRegistryClient',
    Flow = 'Flow'
}

export interface SelectOption {
    text: string;
    value: string | null;
    description?: string;
    disabled?: boolean;
}

export function isDefinedAndNotNull<T>() {
    return (source$: Observable<null | undefined | T>) =>
        source$.pipe(
            filter((input: null | undefined | T): input is T => {
                return input !== null && typeof input !== 'undefined';
            })
        );
}

export * from './components';
export * from './directives';
export * from './pipes';
export * from './services';
export * from './state';
