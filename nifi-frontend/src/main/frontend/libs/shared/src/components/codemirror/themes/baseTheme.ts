/*!
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import type { StyleSpec } from 'style-mod';

const boxStyle = {
    boxShadow: 'var(--mat-sys-level2)',
    borderRadius: '4px'
};

export const codeFontStyle = {
    color: 'var(--editor-text)',
    background: 'transparent',
    fontSize: '12px',
    lineHeight: '18px',
    fontFamily: "Menlo, apercu-mono-regular, Monaco, Consolas, 'Courier New', monospace",
    fontVariantLigatures: 'none',
    overscrollBehaviorInline: 'contain'
};

export const baseTheme: { [selector: string]: StyleSpec } = {
    /**
     * We are using `maxHeight` purposefully here to allow CM6 to fully
     * control the height of the editor. If we manually set the height
     * of the editor we see issues with widget extensions properly
     * calculating their dimensions. This is especially acute when dealing
     * with React based extensions.
     */
    '&': {
        maxWidth: '100%',
        maxHeight: '100%',
        backgroundColor: 'var(--mat-sys-surface-container-low)'
    },

    '.cm-scroller': codeFontStyle,
    '.cm-panels': {
        color: 'var(--mat-sys-on-surface)'
    },
    '.cm-panel > div': {
        borderColor: 'var(--mat-sys-outline)'
    },
    '.cm-panels-top': {
        'z-index': 1,
        border: 'none'
    },
    '.cm-search-container': {
        ...boxStyle,
        backgroundColor: 'var(--mat-sys-surface)',
        position: 'absolute',
        right: '20px',
        top: '0'
    },
    '.cm-search-container:first-child': {
        borderRadius: '6px'
    },
    '.cm-cursor': {
        borderLeftColor: 'var(--mat-sys-on-surface)'
    },
    '.cm-content': {
        // So it will grow to fill the flex container instead of starting out too wide if there's a long
        // line in the editor
        width: 0,
        caretColor: 'var(--mat-sys-on-surface)'
    },
    '&.cm-editor.cm-content': codeFontStyle,
    '&.cm-editor.cm-focused': {
        outline: 'none'
    },
    '.cm-placeholder': {
        color: 'var(--editor-comment)'
    },
    '.cm-line': {
        paddingLeft: '8px'
    },
    '.cm-gutters': {
        // Don't set the gutter background to 'transparent', because then long lines will appear behind
        // the gutter when scrolling horizontally (if line wrapping is off).
        backgroundColor: 'var(--mat-sys-surface-container-high)',
        borderRight: '1px solid',
        cursor: 'default',
        userSelect: 'none'
    },
    '.cm-foldGutter': {
        borderRight: '0',
        paddingRight: '4px'
    },
    '.cm-foldGutter .cm-gutterElement': {
        cursor: 'pointer',
        height: '16px',
        width: '16px'
    },
    '.cm-foldGutter .cm-gutterElement span[title="Fold line"]': {
        transform: 'rotate(90deg)'
    },
    '.cm-foldGutter .cm-gutterElement span[title="Unfold line"]': {
        transform: 'rotate(0deg)'
    },
    '.cm-foldGutter:hover .cm-gutterElement span': {
        opacity: '1'
    },
    '.cm-foldGutter .cm-gutterElement.cm-activeLineGutter': {
        cursor: 'default'
    },
    '.cm-lineNumbers': {
        fontSize: '13px',
        color: 'var(--editor-line-number)',
        cursor: 'default',
        top: '1px',
        paddingLeft: '22px',
        paddingRight: '4px'
    },
    '.cm-activeLineGutter': {
        background: 'none',
        color: 'var(--editor-active-line-number)'
    },
    '.cm-activeLine': {
        background: 'none'
    },
    '.cm-matchingBracket': {
        color: 'var(--mat-sys-inverse-on-surface)',
        background: 'var(--nf-neutral)',
        borderRadius: '2px'
    },
    '&.cm-focused .cm-matchingBracket': {
        backgroundColor: 'var(--nf-neutral)'
    },
    '.cm-nonmatchingBracket': {
        color: 'var(--editor-text)'
    },
    '&.cm-focused .cm-nonmatchingBracket': {
        color: 'var(--mat-sys-inverse-on-surface)',
        background: 'var(--nf-neutral)'
    },
    '.cm-tooltip-autocomplete.cm-tooltip': {
        ...boxStyle,
        borderRadius: '4px',
        maxHeight: '408px',
        zIndex: '1001',
        background: 'var(--mat-sys-surface)',
        border: '1px solid var(--mat-sys-outline)',
        padding: '0',
        marginLeft: '-13px',
        fontStyle: 'inherit',
        fontFamily: 'Inter',
        fontSize: '12px'
    },
    '.cm-tooltip-autocomplete.cm-tooltip ul li': {
        whiteSpace: 'normal',
        borderTop: 'none',
        borderBottom: 'none',
        lineHeight: '1.1',
        padding: '5px 12px',
        fontVariantLigatures: 'none',
        fontFamily: 'Inter',
        color: 'var(--editor-text)',
        position: 'relative',
        alignItems: 'center'
    },
    '.cm-tooltip-autocomplete.cm-tooltip ul li .typetag': {
        textAlign: 'right',
        justifySelf: 'end',
        gridColumn: '2',
        marginRight: '4px',
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis'
    },
    '.darkMode & .cm-tooltip-autocomplete.cm-tooltip ul li .cm-completionLabel': {
        color: 'var(--mat-sys-neutral)'
    },

    '.cm-tooltip-autocomplete.cm-tooltip ul li:first-child': {
        // Shift this up so that the tops of the autocomplete and completion info popups line up.
        transform: 'translate(0px, -1px)'
    },
    '.cm-tooltip-autocomplete.cm-tooltip ul li:last-child': {
        borderBottom: 'none'
    },
    '.cm-tooltip-autocomplete ul li[aria-selected]': {
        background: 'var(--editor-selected-background)',
        borderTop: 'none',
        borderBottom: 'none'
    },
    '.darkMode & .cm-tooltip-autocomplete ul li[aria-selected]': {
        background: 'var(--mat-sys-neutral10)'
    },
    // The left-side blue selection indicator in the autocomplete popup.
    '.cm-tooltip-autocomplete ul li[aria-selected]:before': {
        content: '""',
        background: 'var(--editor-selected-ui)',
        top: 0,
        left: 0,
        position: 'absolute',
        height: '100%',
        width: '5px'
    },
    '.cm-tooltip': {
        background: 'var(--mat-sys-surface)'
    },
    '.cm-tooltip .cm-signature-activeParameter': {
        fontWeight: 'bold',
        color: 'var(--editor-selected-hover-text)'
    },
    '.cm-tooltip-hover': {
        backgroundColor: 'transparent',
        border: 'none'
    },
    '.cm-tooltip-hover > .hover-scroll, .cm-tooltip-hover .cm-diagnostic': {
        ...boxStyle,
        fontFamily:
            'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif' +
            ', "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"',
        overflow: 'hidden',
        background: 'var(--mat-sys-surface)',
        padding: '5px 12px'
    },
    '.cm-tooltip-lint': {
        ...codeFontStyle,
        ...boxStyle,
        background: 'var(--mat-sys-surface)',
        border: '0',
        marginBottom: '4px',
        maxWidth: '450px',
        width: 'max-content'
    },
    '.cm-lint-marker-error': {
        background: 'var(--mat-sys-error)'
    },
    '.cm-lint-marker-warning': {
        background: 'var(--nf-caution-default)'
    },
    '.cm-lint-marker-info': {
        background: '#0000ce'
    },
    '.cm-tooltip.cm-completionInfo, .cm-tooltip-hover .hover-scroll': {
        ...boxStyle,
        // Move shadow to the right to avoid putting shadow on top of the autocomplete box.
        boxShadow: '2px 2px 8px #00000026',
        border: '0',
        display: 'block',
        fontFamily: 'Inter',
        fontSize: '13px',
        fontStyle: 'normal',
        margin: '0',
        maxHeight: '320px',
        minWidth: '420px',
        maxWidth: '450px',
        overflow: 'auto',
        // Shift completion info up without using "top", which Codemirror dynamically sets.
        transform: 'translateY(-1px)',
        wordBreak: 'break-word'
    },
    // Overrides maxHeight when the user drags the handle to resize the popup.
    '.cm-tooltip.cm-completionInfo[style*=" height"]': {
        maxHeight: 'unset !important'
    },
    // Overrides maxWidth when the user drags the handle to resize the popup.
    '.cm-tooltip.cm-completionInfo[style*=" width"]': {
        maxWidth: 'unset !important'
    },
    '.cm-tooltip-autocomplete-info': {
        display: 'flex',
        flexDirection: 'column',
        gap: '4px',
        padding: '0'
    },
    '.cm-tooltip-autocomplete-info code, .cm-tooltip-hover code': {
        fontFamily: "apercu-mono-regular, Menlo, Monaco, Consolas, 'Courier New', monospace",
        color: 'var(--editor-text)',
        fontSize: '13px',
        whiteSpace: 'pre-wrap',
        margin: '0'
    },
    '.cm-tooltip-autocomplete-info pre:first-of-type': {
        margin: '4px 0'
    },
    '.cm-tooltip-autocomplete-info p': {
        margin: '0',
        lineHeight: '1.5'
    },
    '.cm-tooltip-autocomplete-info p:last-of-type': {
        'margin-bottom': '4px'
    },
    '.cm-tooltip-autocomplete-info hr:first-of-type': {
        border: 'none',
        height: '1px',
        backgroundColor: 'var(--mat-sys-outline)',
        margin: '4px 0'
    },
    '.cm-tooltip-autocomplete.cm-tooltip > ul': {
        borderRadius: '4px',
        maxHeight: '15em',
        minWidth: '420px'
    },
    '.cm-tooltip-autocomplete.cm-tooltip > ul > li': {
        display: 'grid',
        columnGap: '8px',
        gridTemplateColumns: 'auto auto'
    },
    '.cm-completionIcon': { display: 'none' },
    '.cm-completionLabel': {
        display: 'block',
        fontFamily: "Menlo, apercu-mono-regular, Monaco, Consolas, 'Courier New', monospace",
        fontSize: '13.5px',
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        gridColumn: '1'
    },
    '.cm-selectionBackground': {
        background: 'var(--editor-selected-background)',
        opacity: '.4',
        transform: 'translateY(-2px)',
        'padding-top': '2px',
        'padding-bottom': '2px'
    },
    '&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground': {
        background: 'var(--editor-selected-background)',
        opacity: '.6'
    },
    '.cm-selectionMatch': {
        background: 'var(--editor-selection-match-bg)'
    },

    // Search colors override other font/background colors, so "!important" is used.
    '.cm-searchMatch *': {
        background: 'var(--editor-search-match-bg) !important'
    },
    '.cm-searchMatch-selected *': {
        background: 'var(--editor-search-match-selected-bg) !important'
    },
    '.darkMode & .cm-searchMatch *': {
        color: `var(--mat-sys-on-surface) !important`
    },
    '.darkMode & .cm-searchMatch-selected *': {
        color: 'var(--mat-sys-surface-bright) !important'
    },

    '.cm-lintRange': {
        paddingBottom: '1.7px'
    },
    '.cm-lintRange-error': {
        backgroundImage: 'none',
        borderBottom: '2px dotted var(--mat-sys-error)',
        paddingBottom: 0
    },
    '.cm-lintRange-info': {
        backgroundImage: 'none',
        borderBottom: '2px dotted #0000ce',
        paddingBottom: 0
    },
    '.cm-lintRange-warning': {
        backgroundImage: 'none',
        borderBottom: '2px dotted var(--nf-caution-default)',
        paddingBottom: 0
    },
    '.cm-lintPoint-error:after': {
        borderBottom: '6px solid var(--mat-sys-error)',
        bottom: '-3px',
        left: '-6px'
    },
    '.desclink:link, .desclink:visited': {
        color: 'var(--editor-comment)',
        textDecoration: 'underline'
    },
    '.desclink:hover': {
        color: '#0000ce'
    },
    '.active-handler': {
        backgroundColor: 'transparent',
        borderColor: 'var(--editor-active-handler-border)',
        borderWidth: '1px',
        borderStyle: 'solid',
        borderRadius: '6px',
        padding: '2px 4px'
    },
    '.active-handler-tooltip.hover-scroll': {
        borderLeftColor: 'var(--editor-control-keyword)',
        borderLeftWidth: '5px',
        borderLeftStyle: 'solid'
    },
    '.error-highlight': { background: 'var(--editor-error-bg)' },
    '.executed-highlight': { background: 'var(--editor-executed-bg)' },
    '.executed-line': {
        borderLeft: '#457cff 4px solid',
        width: '4px',
        height: '100%'
    },
    '.highlight-query-range-gutter': {
        marginLeft: '-1px'
    },
    '.assistant-range-highlight': {
        backgroundColor: '#c6c6c661'
    },
    '.darkMode & .assistant-range-highlight': {
        backgroundColor: '#404d6b5c'
    },
    '.assistant-diff-additions-chunk': {
        userSelect: 'text'
    },
    '.assistant-diff-additions-chunk::before': {
        position: 'relative'
    },
    '.assistant-diff-added-line': {
        backgroundColor: 'var(--nf-success-default-background)',
        padding: '0 2px 0 3px',
        borderLeft: '4px solid var(--nf-success-default)',
        marginLeft: '1px'
    },
    '.assistant-diff-deleted-line': {
        backgroundColor: 'var(--mat-sys-error-container)',
        padding: '0 2px 0 3px',
        borderLeft: '4px solid var(--mat-sys-error)',
        marginLeft: '1px'
    },
    '.assistant-diff-added-line > ins': {
        textDecoration: 'unset'
    }
};
