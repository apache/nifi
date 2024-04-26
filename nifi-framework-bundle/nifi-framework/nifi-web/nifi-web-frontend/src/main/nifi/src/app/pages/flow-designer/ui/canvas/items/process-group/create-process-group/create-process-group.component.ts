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

import { Component, ElementRef, Inject, ViewChild } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { CreateProcessGroupDialogRequest } from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { createProcessGroup, uploadProcessGroup } from '../../../../../state/flow/flow.actions';
import { SelectOption } from '../../../../../../../state/shared';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';
import { ErrorBanner } from '../../../../../../../ui/common/error-banner/error-banner.component';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { MatIconModule } from '@angular/material/icon';
import { NiFiCommon } from '../../../../../../../service/nifi-common.service';

@Component({
    selector: 'create-process-group',
    standalone: true,
    imports: [
        AsyncPipe,
        ErrorBanner,
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
    templateUrl: './create-process-group.component.html',
    styleUrls: ['./create-process-group.component.scss']
})
export class CreateProcessGroup {
    saving$ = this.store.select(selectSaving);

    protected readonly TextTip = TextTip;

    @ViewChild('flowUploadControl') flowUploadControl!: ElementRef;

    createProcessGroupForm: FormGroup;
    parameterContextsOptions: SelectOption[] = [];

    flowNameAttached: string | null = null;
    flowDefinition: File | null = null;

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: CreateProcessGroupDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>,
        private nifiCommon: NiFiCommon
    ) {
        this.parameterContextsOptions.push({
            text: 'No parameter context',
            value: null
        });

        dialogRequest.parameterContexts.forEach((parameterContext) => {
            if (parameterContext.permissions.canRead) {
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

    attachFlow(event: Event): void {
        const target = event.target as HTMLInputElement;
        const files = target.files as FileList;
        const file = files.item(0);
        if (file) {
            this.createProcessGroupForm
                .get('newProcessGroupName')
                ?.setValue(this.nifiCommon.substringBeforeLast(file.name, '.'));
            this.createProcessGroupForm.get('newProcessGroupName')?.markAsDirty();
            this.createProcessGroupForm.get('newProcessGroupParameterContext')?.setValue(null);
            this.flowNameAttached = file.name;
            this.flowDefinition = file;
        }
    }

    removeAttachedFlow(): void {
        this.createProcessGroupForm.get('newProcessGroupName')?.setValue('');
        this.flowUploadControl.nativeElement.value = '';
        this.flowNameAttached = null;
        this.flowDefinition = null;
    }

    createProcessGroup(): void {
        if (this.flowDefinition) {
            this.store.dispatch(
                uploadProcessGroup({
                    request: {
                        ...this.dialogRequest.request,
                        name: this.createProcessGroupForm.get('newProcessGroupName')?.value,
                        flowDefinition: this.flowDefinition
                    }
                })
            );
        } else {
            this.store.dispatch(
                createProcessGroup({
                    request: {
                        ...this.dialogRequest.request,
                        name: this.createProcessGroupForm.get('newProcessGroupName')?.value,
                        parameterContextId: this.createProcessGroupForm.get('newProcessGroupParameterContext')?.value
                    }
                })
            );
        }
    }
}
