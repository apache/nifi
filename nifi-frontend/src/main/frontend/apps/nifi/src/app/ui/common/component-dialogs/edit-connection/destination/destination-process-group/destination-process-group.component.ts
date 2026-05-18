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

import { Component, forwardRef, Input } from '@angular/core';
import { ControlValueAccessor, FormsModule, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatCheckboxModule } from '@angular/material/checkbox';

import { MatFormFieldModule } from '@angular/material/form-field';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiTooltipDirective, TextTip, SelectOption } from '@nifi/shared';

@Component({
    selector: 'destination-process-group',
    templateUrl: './destination-process-group.component.html',
    styleUrls: ['./destination-process-group.component.scss'],
    imports: [
        MatCheckboxModule,
        FormsModule,
        MatFormFieldModule,
        MatOptionModule,
        MatSelectModule,
        NifiTooltipDirective
    ],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => DestinationProcessGroup),
            multi: true
        }
    ]
})
export class DestinationProcessGroup implements ControlValueAccessor {
    @Input() set processGroup(processGroup: any) {
        if (processGroup) {
            if (processGroup.permissions.canRead) {
                this.groupName = processGroup.component.name;
            } else {
                this.groupName = processGroup.id;
            }
        }
    }
    @Input() set inputPorts(inputPorts: any[]) {
        if (inputPorts) {
            this.noPorts = inputPorts.length == 0;
            this.hasUnauthorizedPorts = false;

            this.inputPortItems = inputPorts
                .filter((inputPort) => {
                    const authorized: boolean = inputPort.permissions.canRead && inputPort.permissions.canWrite;
                    if (!authorized) {
                        this.hasUnauthorizedPorts = true;
                    }
                    return authorized;
                })
                .filter((inputPort) => !inputPort.allowRemoteAccess)
                .map((inputPort) => {
                    return {
                        value: inputPort.id,
                        text: inputPort.component.name,
                        description: inputPort.component.comments
                    };
                });
        }
    }

    protected readonly TextTip = TextTip;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (outputPort: string) => void;

    groupName!: string;
    inputPortItems!: SelectOption[];
    selectedInputPort!: string;

    noPorts = false;
    hasUnauthorizedPorts = false;

    registerOnChange(onChange: (selectedInputPort: string) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(selectedInputPort: string): void {
        this.selectedInputPort = selectedInputPort;
    }

    handleChanged() {
        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.selectedInputPort);
    }
}
