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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import {
    MAT_DIALOG_DATA,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    MatDialogTitle
} from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { Observable } from 'rxjs';
import { MatButton, MatIconButton } from '@angular/material/button';
import { NifiTooltipDirective, TextTip, MapTable, CloseOnEscapeDialog, MapTableEntry } from '@nifi/shared';

export interface ReferencedAttributesDialogData {
    attributes: MapTableEntry[];
}

@Component({
    selector: 'referenced-attributes-dialog',
    imports: [
        CommonModule,
        MatDialogTitle,
        ReactiveFormsModule,
        MatDialogContent,
        MatDialogActions,
        MatButton,
        MatDialogClose,
        MapTable,
        NifiTooltipDirective,
        MatIconButton
    ],
    templateUrl: './referenced-attributes-dialog.component.html',
    styleUrl: './referenced-attributes-dialog.component.scss'
})
export class ReferencedAttributesDialog extends CloseOnEscapeDialog {
    referencedAttributesForm: FormGroup;

    @Input() createNew!: (existingEntries: string[]) => Observable<MapTableEntry>;
    @Output() verify = new EventEmitter<any>();

    constructor(
        private formBuilder: FormBuilder,
        @Inject(MAT_DIALOG_DATA) private data: ReferencedAttributesDialogData
    ) {
        super();
        const attributes: MapTableEntry[] = data.attributes || [];
        this.referencedAttributesForm = this.formBuilder.group({
            attributes: new FormControl(attributes)
        });
    }

    verifyClicked() {
        this.verify.next(this.referencedAttributesForm.value);
    }

    clearAttributesClicked() {
        this.referencedAttributesForm.reset();
    }

    isEmpty() {
        const attributes = this.referencedAttributesForm.get('attributes')?.value || [];
        return attributes.length === 0;
    }

    protected readonly TextTip = TextTip;
}
