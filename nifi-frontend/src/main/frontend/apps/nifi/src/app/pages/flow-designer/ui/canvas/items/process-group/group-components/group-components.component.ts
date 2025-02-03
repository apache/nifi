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
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { GroupComponentsDialogRequest } from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { groupComponents } from '../../../../../state/flow/flow.actions';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ComponentType, SelectOption, TextTip, NifiTooltipDirective } from '@nifi/shared';
import { MatIconModule } from '@angular/material/icon';
import { Client } from '../../../../../../../service/client.service';

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
    saving$ = this.store.select(selectSaving);

    protected readonly TextTip = TextTip;

    createProcessGroupForm: FormGroup;
    parameterContextsOptions: SelectOption[] = [];

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: GroupComponentsDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>,
        private client: Client
    ) {
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
            newProcessGroupParameterContext: new FormControl(dialogRequest.currentParameterContextId)
        });
    }

    createProcessGroup(): void {
        this.store.dispatch(
            groupComponents({
                request: {
                    revision: {
                        version: 0,
                        clientId: this.client.getClientId()
                    },
                    type: ComponentType.ProcessGroup,
                    position: this.dialogRequest.request.position,
                    name: this.createProcessGroupForm.get('newProcessGroupName')?.value,
                    parameterContextId: this.createProcessGroupForm.get('newProcessGroupParameterContext')?.value,
                    components: this.dialogRequest.request.moveComponents
                }
            })
        );
    }
}
