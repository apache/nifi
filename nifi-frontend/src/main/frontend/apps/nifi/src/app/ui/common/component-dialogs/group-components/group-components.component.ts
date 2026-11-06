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
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { GroupComponentsDialogRequest, GroupComponentsRequest } from '../../../../state/flow-shared';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ComponentType, NifiSpinnerDirective, SelectOption, TextTip, NifiTooltipDirective } from '@nifi/shared';
import { MatIconModule } from '@angular/material/icon';
import { Client } from '../../../../service/client.service';
import { Observable, of } from 'rxjs';

@Component({
    selector: 'group-components',
    imports: [
        AsyncPipe,
        MatButtonModule,
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        NifiSpinnerDirective,
        ReactiveFormsModule,
        MatOptionModule,
        MatSelectModule,
        NifiTooltipDirective,
        MatIconModule
    ],
    templateUrl: './group-components.component.html',
    styleUrls: ['./group-components.component.scss']
})
export class GroupComponents {
    private dialogRequest = inject<GroupComponentsDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private client = inject(Client);

    @Input() saving$: Observable<boolean> = of(false);
    @Input() supportsParameters = true;
    @Output() groupComponents = new EventEmitter<GroupComponentsRequest>();

    protected readonly TextTip = TextTip;

    createProcessGroupForm: FormGroup;
    parameterContextsOptions: SelectOption[] = [];

    constructor() {
        const dialogRequest = this.dialogRequest;

        this.parameterContextsOptions.push({
            text: 'No parameter context',
            value: null
        });

        dialogRequest.parameterContexts.forEach((parameterContext) => {
            if (parameterContext.permissions.canRead && parameterContext.component) {
                this.parameterContextsOptions.push({
                    text: parameterContext.component.name,
                    value: parameterContext.id,
                    description: parameterContext.component.description
                });
            }
        });

        this.createProcessGroupForm = this.formBuilder.group({
            newProcessGroupName: new FormControl('', Validators.required),
            newProcessGroupParameterContext: new FormControl<string | null>(
                dialogRequest.currentParameterContextId ?? null
            )
        });
    }

    submitGroupComponents(): void {
        this.groupComponents.emit({
            revision: {
                version: 0,
                clientId: this.client.getClientId()
            },
            type: ComponentType.ProcessGroup,
            position: this.dialogRequest.request.position,
            name: this.createProcessGroupForm.get('newProcessGroupName')?.value,
            parameterContextId: this.supportsParameters
                ? this.createProcessGroupForm.get('newProcessGroupParameterContext')?.value
                : null,
            components: this.dialogRequest.request.moveComponents
        });
    }
}
