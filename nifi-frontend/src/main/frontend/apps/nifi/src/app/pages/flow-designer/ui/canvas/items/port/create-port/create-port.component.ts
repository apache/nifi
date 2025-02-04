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
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { selectParentProcessGroupId, selectSaving } from '../../../../../state/flow/flow.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { createPort } from '../../../../../state/flow/flow.actions';
import { CreateComponentRequest } from '../../../../../state/flow';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { ComponentType, SelectOption, NifiTooltipDirective, TextTip, CloseOnEscapeDialog } from '@nifi/shared';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'create-port',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatSelectModule,
        MatButtonModule,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        ContextErrorBanner
    ],
    templateUrl: './create-port.component.html',
    styleUrls: ['./create-port.component.scss']
})
export class CreatePort extends CloseOnEscapeDialog {
    saving$ = this.store.select(selectSaving);

    protected readonly TextTip = TextTip;

    createPortForm: FormGroup;
    isRootProcessGroup = false;
    portTypeLabel: string;

    allowRemoteAccessOptions: SelectOption[] = [
        {
            text: 'Local connections',
            value: 'false',
            description: 'Receive FlowFiles from components in parent process groups'
        },
        {
            text: 'Remote connections (site-to-site)',
            value: 'true',
            description: 'Receive FlowFiles from remote process group (site-to-site)'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: CreateComponentRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>
    ) {
        super();
        // set the port type name
        if (ComponentType.InputPort == this.request.type) {
            this.portTypeLabel = 'Input Port';
        } else {
            this.portTypeLabel = 'Output Port';
        }

        // build the form
        this.createPortForm = this.formBuilder.group({
            newPortName: new FormControl('', Validators.required),
            newPortAllowRemoteAccess: new FormControl(this.allowRemoteAccessOptions[0].value, Validators.required)
        });

        // listen for changes to the parent process group id
        this.store
            .select(selectParentProcessGroupId)
            .pipe(takeUntilDestroyed())
            .subscribe((parentProcessGroupId) => {
                this.isRootProcessGroup = parentProcessGroupId == null;
            });
    }

    createPort() {
        this.store.dispatch(
            createPort({
                request: {
                    ...this.request,
                    name: this.createPortForm.get('newPortName')?.value,
                    allowRemoteAccess: this.createPortForm.get('newPortAllowRemoteAccess')?.value
                }
            })
        );
    }

    protected readonly ComponentType = ComponentType;
    protected readonly ErrorContextKey = ErrorContextKey;
}
