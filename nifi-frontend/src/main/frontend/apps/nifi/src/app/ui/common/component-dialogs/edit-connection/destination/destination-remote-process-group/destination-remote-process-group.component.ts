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
    selector: 'destination-remote-process-group',
    templateUrl: './destination-remote-process-group.component.html',
    styleUrls: ['./destination-remote-process-group.component.scss'],
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
            useExisting: forwardRef(() => DestinationRemoteProcessGroup),
            multi: true
        }
    ]
})
export class DestinationRemoteProcessGroup implements ControlValueAccessor {
    @Input() set remoteProcessGroup(remoteProcessGroup: any) {
        if (remoteProcessGroup) {
            const rpg = remoteProcessGroup.component;
            const inputPorts: any[] = rpg.contents.inputPorts;

            if (inputPorts) {
                this.noPorts = inputPorts.length == 0;

                this.inputPortItems = inputPorts.map((inputPort) => {
                    return {
                        value: inputPort.id,
                        text: inputPort.name,
                        description: inputPort.comments,
                        disabled: inputPort.exists === false
                    };
                });
            }

            this.groupName = rpg.name;
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
