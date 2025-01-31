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

import { Component, DestroyRef, inject, Input, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { AsyncPipe } from '@angular/common';
import { CdkMenu, CdkMenuItem, CdkMenuTrigger } from '@angular/cdk/menu';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

export interface ContextMenuDefinitionProvider {
    getMenu(menuId: string): ContextMenuDefinition | undefined;

    filterMenuItem(menuItem: ContextMenuItemDefinition): boolean;

    menuItemClicked(menuItem: ContextMenuItemDefinition, event: MouseEvent): void;
}

export interface KeyboardShortcut {
    code: string;
    control?: boolean;
    shift?: boolean;
    alt?: boolean;
}

export interface ContextMenuItemDefinition {
    isSeparator?: boolean;
    condition?: (selection: any) => boolean;
    clazz?: string;
    text?: string;
    subMenuId?: string;
    action?: (selection: any, event?: MouseEvent) => void;
    shortcut?: KeyboardShortcut;
}

export interface ContextMenuDefinition {
    id: string;
    menuItems: ContextMenuItemDefinition[];
}

@Component({
    selector: 'fd-context-menu',
    templateUrl: './context-menu.component.html',
    imports: [AsyncPipe, CdkMenu, CdkMenuItem, CdkMenuTrigger],
    styleUrls: ['./context-menu.component.scss']
})
export class ContextMenu implements OnInit, OnDestroy {
    private destroyRef = inject(DestroyRef);

    @Input() menuProvider!: ContextMenuDefinitionProvider;
    @Input() menuId: string | undefined;
    @ViewChild('menu', { static: true }) menu!: TemplateRef<any>;

    private showFocused: Subject<boolean> = new Subject();
    showFocused$: Observable<boolean> = this.showFocused.asObservable().pipe(takeUntilDestroyed(this.destroyRef));
    isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;

    getMenuItems(menuId: string | undefined): ContextMenuItemDefinition[] {
        if (menuId) {
            const menuDefinition: ContextMenuDefinition | undefined = this.menuProvider.getMenu(menuId);

            if (menuDefinition) {
                // find all applicable menu items
                let applicableMenuItems = menuDefinition.menuItems.filter((menuItem: ContextMenuItemDefinition) => {
                    // include if the condition matches
                    if (menuItem.condition) {
                        return this.menuProvider.filterMenuItem(menuItem);
                    }

                    // include if the sub menu has items
                    if (menuItem.subMenuId) {
                        return this.getMenuItems(menuItem.subMenuId).length > 0;
                    }

                    return true;
                });

                // remove any extra separators
                applicableMenuItems = applicableMenuItems.filter(
                    (menuItem: ContextMenuItemDefinition, index: number) => {
                        if (menuItem.isSeparator && index > 0) {
                            // cannot have two consecutive separators
                            return !applicableMenuItems[index - 1].isSeparator;
                        }

                        return true;
                    }
                );

                return applicableMenuItems.filter((menuItem: ContextMenuItemDefinition, index: number) => {
                    if (menuItem.isSeparator) {
                        // a separator cannot be first
                        if (index === 0) {
                            return false;
                        }

                        // a separator cannot be last
                        if (index >= applicableMenuItems.length - 1) {
                            return false;
                        }
                    }

                    return true;
                });
            } else {
                return [];
            }
        }

        return [];
    }

    hasSubMenu(menuItemDefinition: ContextMenuItemDefinition): boolean {
        return !!menuItemDefinition.subMenuId;
    }

    keydown(event: KeyboardEvent): void {
        // TODO - Currently the first item in the context menu is auto focused. By default, this is rendered with an
        // outline. This appears to be an issue with the cdkMenu/cdkMenuItem so we are working around it by manually
        // overriding styles.
        if (event.key === 'Escape') {
            event.stopPropagation();
        }
        this.showFocused.next(true);
    }

    ngOnInit(): void {
        this.showFocused.next(false);
    }

    menuItemClicked(menuItem: ContextMenuItemDefinition, event: MouseEvent) {
        this.menuProvider.menuItemClicked(menuItem, event);
    }

    ngOnDestroy(): void {
        this.showFocused.complete();
    }
}
