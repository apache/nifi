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

import { Component, Input } from '@angular/core';
import { ComponentType, NiFiCommon, TextTip } from '@nifi/shared';
import { Observable } from 'rxjs';
import { AsyncPipe } from '@angular/common';
import { NiFiState } from '../../../../../state';
import { Store } from '@ngrx/store';
import { selectExtensionFromTypes } from '../../../../../state/extension-types/extension-types.selectors';
import { RouterLink } from '@angular/router';
import { LoadExtensionTypesForDocumentationResponse } from '../../../../../state/extension-types';
import { ComponentDocumentedType } from '../../../state';
import { DocumentedType } from '../../../../../state/shared';

@Component({
    selector: 'see-also',
    templateUrl: './see-also.component.html',
    imports: [RouterLink, AsyncPipe],
    styleUrl: './see-also.component.scss'
})
export class SeeAlsoComponent {
    @Input() extensionTypes: string[] | null = null;

    constructor(
        private store: Store<NiFiState>,
        private nifiCommon: NiFiCommon
    ) {}

    getProcessorFromType(extensionTypes: string[]): Observable<LoadExtensionTypesForDocumentationResponse> {
        return this.store.select(selectExtensionFromTypes(extensionTypes));
    }

    mapExtensionPoint(
        extensionType: string,
        documentedTypes: LoadExtensionTypesForDocumentationResponse
    ): ComponentDocumentedType | undefined {
        let documentedType: DocumentedType | undefined;

        documentedType = documentedTypes.processorTypes.find((documentedType) => extensionType === documentedType.type);
        if (documentedType) {
            return {
                documentedType,
                componentType: ComponentType.Processor
            };
        }

        documentedType = documentedTypes.controllerServiceTypes.find(
            (documentedType) => extensionType === documentedType.type
        );
        if (documentedType) {
            return {
                documentedType,
                componentType: ComponentType.ControllerService
            };
        }

        documentedType = documentedTypes.reportingTaskTypes.find(
            (documentedType) => extensionType === documentedType.type
        );
        if (documentedType) {
            return {
                documentedType,
                componentType: ComponentType.ReportingTask
            };
        }

        documentedType = documentedTypes.parameterProviderTypes.find(
            (documentedType) => extensionType === documentedType.type
        );
        if (documentedType) {
            return {
                documentedType,
                componentType: ComponentType.ParameterProvider
            };
        }

        return undefined;
    }

    formatExtensionName(extensionType: string): string {
        return this.nifiCommon.getComponentTypeLabel(extensionType);
    }

    protected readonly TextTip = TextTip;
    protected readonly ComponentType = ComponentType;
}
