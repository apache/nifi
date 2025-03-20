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

import { Component, EventEmitter, Inject, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import {
    MAT_DIALOG_DATA,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    MatDialogTitle
} from '@angular/material/dialog';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatButton } from '@angular/material/button';
import { ChangeColorRequest } from '../../../state/flow';
import { MatFormField, MatLabel } from '@angular/material/form-field';
import { MatInput } from '@angular/material/input';
import { CanvasUtils } from '../../../service/canvas-utils.service';
import { ComponentType, NiFiCommon, CloseOnEscapeDialog } from '@nifi/shared';
import { MatCheckbox } from '@angular/material/checkbox';

@Component({
    selector: 'change-component-dialog',
    imports: [
        CommonModule,
        MatDialogTitle,
        ReactiveFormsModule,
        MatDialogContent,
        MatButton,
        MatDialogActions,
        MatDialogClose,
        MatFormField,
        MatLabel,
        MatInput,
        MatCheckbox
    ],
    templateUrl: './change-color-dialog.component.html',
    styleUrl: './change-color-dialog.component.scss'
})
export class ChangeColorDialog extends CloseOnEscapeDialog {
    color: string | null = null;
    noColor: boolean = true;
    contrastColor: string | null = null;
    type: ComponentType | null = null;
    changeColorForm: FormGroup;
    private _data: ChangeColorRequest[] = [];
    private DEFAULT_LABEL_COLOR = '#fff7d7';

    @Output() changeColor = new EventEmitter<ChangeColorRequest[]>();

    constructor(
        @Inject(MAT_DIALOG_DATA) private data: ChangeColorRequest[],
        private canvasUtils: CanvasUtils,
        private nifiCommon: NiFiCommon,
        private formBuilder: FormBuilder
    ) {
        super();
        this._data = data;
        let isDefaultColor = true;

        if (data.length > 0) {
            this.color = data[0].color;
            if (this.color !== null) {
                const hex = this.nifiCommon.substringAfterLast(this.color, '#');
                this.contrastColor = this.canvasUtils.determineContrastColor(hex);
                this.noColor = false;
                isDefaultColor = false;
            }
            this.type = data[0].type;
            if (this.type === ComponentType.Label && this.color === null) {
                this.color = this.DEFAULT_LABEL_COLOR;
                const hex = this.nifiCommon.substringAfterLast(this.color, '#');
                this.contrastColor = this.canvasUtils.determineContrastColor(hex);
                isDefaultColor = true;
            }
        }

        this.changeColorForm = formBuilder.group({
            color: [this.color],
            noColor: [isDefaultColor]
        });
    }

    colorChanged(): void {
        this.color = this.changeColorForm.get('color')?.value;
        if (this.color !== null) {
            const hex = this.nifiCommon.substringAfterLast(this.color, '#');
            this.contrastColor = this.canvasUtils.determineContrastColor(hex);
            this.noColor = false;
        } else {
            this.contrastColor = null;
        }
    }

    noColorChanged(): void {
        const noColorChecked = this.changeColorForm.get('noColor')?.value;
        if (noColorChecked) {
            this.noColor = true;
            if (this.type === ComponentType.Label) {
                this.color = this.DEFAULT_LABEL_COLOR;
                const hex = this.nifiCommon.substringAfterLast(this.color, '#');
                this.contrastColor = this.canvasUtils.determineContrastColor(hex);
            } else {
                this.color = null;
                this.contrastColor = null;
            }
        } else {
            this.noColor = false;
            this.color = this.changeColorForm.get('color')?.value;
            if (this.color === null) {
                if (this.type === ComponentType.Label) {
                    this.color = this.DEFAULT_LABEL_COLOR;
                } else {
                    this.color = '#000000'; // default for the color picker
                }
            }
            const hex = this.nifiCommon.substringAfterLast(this.color, '#');
            this.contrastColor = this.canvasUtils.determineContrastColor(hex);
        }
    }

    applyClicked() {
        const result: ChangeColorRequest[] = this._data.map((changeColorRequest) => {
            return {
                ...changeColorRequest,
                color: this.noColor ? null : this.color
            };
        });
        this.changeColor.next(result);
    }

    protected readonly ComponentType = ComponentType;
}
