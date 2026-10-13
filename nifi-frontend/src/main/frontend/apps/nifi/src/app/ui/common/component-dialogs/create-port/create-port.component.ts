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
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { CreateComponentRequest, CreatePortRequest } from '../../../../state/flow-shared';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { ComponentType, NifiSpinnerDirective, NifiTooltipDirective, TextTip, CloseOnEscapeDialog } from '@nifi/shared';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';
import { Observable, of } from 'rxjs';

interface RemoteAccessOption {
    text: string;
    value: boolean;
    description: string;
}

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
    request = inject<CreateComponentRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);

    @Input() saving$: Observable<boolean> = of(false);
    @Input() isRootProcessGroup = false;
    @Output() createPort = new EventEmitter<CreatePortRequest>();

    protected readonly TextTip = TextTip;

    createPortForm: FormGroup;
    portTypeLabel: string;

    allowRemoteAccessOptions: RemoteAccessOption[] = [
        {
            text: 'Local connections',
            value: false,
            description: 'Receive FlowFiles from components in parent process groups'
        },
        {
            text: 'Remote connections (site-to-site)',
            value: true,
            description: 'Receive FlowFiles from remote process group (site-to-site)'
        }
    ];

    constructor() {
        super();
        if (ComponentType.InputPort == this.request.type) {
            this.portTypeLabel = 'Input Port';
        } else {
            this.portTypeLabel = 'Output Port';
        }

        this.createPortForm = this.formBuilder.group({
            newPortName: new FormControl('', Validators.required),
            newPortAllowRemoteAccess: new FormControl<boolean>(this.allowRemoteAccessOptions[0].value, {
                nonNullable: true,
                validators: Validators.required
            })
        });
    }

    submitCreatePort(): void {
        this.createPort.emit({
            ...this.request,
            name: this.createPortForm.get('newPortName')?.value,
            allowRemoteAccess: this.createPortForm.get('newPortAllowRemoteAccess')?.value
        });
    }

    protected readonly ComponentType = ComponentType;
    protected readonly ErrorContextKey = ErrorContextKey;
}
