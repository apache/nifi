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

import { Component, Input } from '@angular/core';
import { ComponentTypeNamePipe } from '../../pipes/component-type-name.pipe';
import { ComponentType } from '../../types';
import { CopyDirective } from '../../directives/copy/copy.directive';

interface IconMeta {
    className: string;
    iconType: 'font-awesome' | 'flowfont';
}

@Component({
    selector: 'component-context',
    imports: [ComponentTypeNamePipe, CopyDirective],
    templateUrl: './component-context.component.html',
    styleUrl: './component-context.component.scss'
})
export class ComponentContext {
    private static readonly DEFAULT_ICON: IconMeta = { className: 'icon-drop', iconType: 'flowfont' };

    private _componentType: ComponentType | string | null = ComponentType.Processor;
    componentIcon: IconMeta = ComponentContext.DEFAULT_ICON;

    @Input() set type(type: ComponentType | string | null) {
        this._componentType = type;
        this.componentIcon = this.getIconMeta(type);
    }

    get type(): ComponentType | string | null {
        return this._componentType;
    }

    @Input() id: string | null = null;
    @Input() name: string | undefined = '';

    private getIconMeta(type: ComponentType | string | null): IconMeta {
        if (type === null) {
            return ComponentContext.DEFAULT_ICON;
        }
        switch (type) {
            case ComponentType.Connection:
                return { className: 'icon-connect', iconType: 'flowfont' };
            case ComponentType.Processor:
                return { className: 'icon-processor', iconType: 'flowfont' };
            case ComponentType.OutputPort:
                return { className: 'icon-port-out', iconType: 'flowfont' };
            case ComponentType.InputPort:
                return { className: 'icon-port-in', iconType: 'flowfont' };
            case ComponentType.ProcessGroup:
                return { className: 'icon-group', iconType: 'flowfont' };
            case ComponentType.Funnel:
                return { className: 'icon-funnel', iconType: 'flowfont' };
            case ComponentType.Label:
                return { className: 'icon-label', iconType: 'flowfont' };
            case ComponentType.RemoteProcessGroup:
                return { className: 'icon-group-remote', iconType: 'flowfont' };
            case ComponentType.Connector:
                return { className: 'fa fa-plug tertiary-color', iconType: 'font-awesome' };
            default:
                return ComponentContext.DEFAULT_ICON;
        }
    }
}
