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

import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { CreateProcessorDialogRequest } from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { createProcessor } from '../../../../../state/flow/flow.actions';
import { ExtensionCreation } from '../../../../../../../ui/common/extension-creation/extension-creation.component';
import { DocumentedType } from '../../../../../../../state/shared';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';

@Component({
    selector: 'create-processor',
    imports: [ExtensionCreation, AsyncPipe],
    templateUrl: './create-processor.component.html',
    styleUrls: ['./create-processor.component.scss']
})
export class CreateProcessor {
    processorTypes: DocumentedType[];
    saving$ = this.store.select(selectSaving);

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: CreateProcessorDialogRequest,
        private store: Store<CanvasState>
    ) {
        this.processorTypes = dialogRequest.processorTypes;
    }

    createProcessor(processorType: DocumentedType): void {
        this.store.dispatch(
            createProcessor({
                request: {
                    ...this.dialogRequest.request,
                    processorType: processorType.type,
                    processorBundle: processorType.bundle
                }
            })
        );
    }
}
