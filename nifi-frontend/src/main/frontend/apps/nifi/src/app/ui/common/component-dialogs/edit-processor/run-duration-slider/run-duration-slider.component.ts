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

import { Component, forwardRef } from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatSliderModule } from '@angular/material/slider';

@Component({
    selector: 'run-duration-slider',
    templateUrl: './run-duration-slider.component.html',
    imports: [MatSliderModule],
    styleUrls: ['./run-duration-slider.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => RunDurationSlider),
            multi: true
        }
    ]
})
export class RunDurationSlider implements ControlValueAccessor {
    runDurationValues: number[] = [0, 25, 50, 100, 250, 500, 1000, 2000];
    runDurationIndex!: number;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (runDuration: number) => void;

    registerOnChange(onChange: (runDuration: number) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    formatLabel(value: number): string {
        if (value === 0) {
            return '0ms';
        } else if (value === 1) {
            return '25ms';
        } else if (value === 2) {
            return '50ms';
        } else if (value === 3) {
            return '100ms';
        } else if (value === 4) {
            return '250ms';
        } else if (value === 5) {
            return '500ms';
        } else if (value === 6) {
            return '1s';
        } else {
            return '2s';
        }
    }

    writeValue(runDuration: number): void {
        const index: number = this.runDurationValues.indexOf(runDuration);
        if (index < 0) {
            this.runDurationIndex = 0;
        } else {
            this.runDurationIndex = index;
        }
    }

    runDurationChanged(value: number): void {
        this.runDurationIndex = value;
        this.handleChanged();
    }

    private handleChanged(): void {
        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.runDurationValues[this.runDurationIndex]);
    }
}
