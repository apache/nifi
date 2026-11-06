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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { CreateProcessorDialogRequest, CreateProcessorRequest } from '../../../../state/flow-shared';
import { ExtensionCreation } from '../../extension-creation/extension-creation.component';
import { DocumentedType } from '../../../../state/shared';
import { AsyncPipe } from '@angular/common';
import { Observable, of } from 'rxjs';
import { ExtensionTypesLoadingStatus } from '../../../../state/extension-types';

@Component({
    selector: 'create-processor',
    imports: [ExtensionCreation, AsyncPipe],
    templateUrl: './create-processor.component.html',
    styleUrls: ['./create-processor.component.scss']
})
export class CreateProcessor {
    private dialogRequest = inject<CreateProcessorDialogRequest>(MAT_DIALOG_DATA);

    @Input() saving$: Observable<boolean> = of(false);
    @Input() processorTypes$: Observable<DocumentedType[]> = of([]);
    @Input() processorTypesLoadingStatus$: Observable<ExtensionTypesLoadingStatus> = of('pending');
    @Output() createProcessor = new EventEmitter<CreateProcessorRequest>();

    onProcessorTypeSelected(processorType: DocumentedType): void {
        this.createProcessor.emit({
            ...this.dialogRequest.request,
            processorType: processorType.type,
            processorBundle: processorType.bundle
        });
    }
}
