# CodeMirror Component API

A developer-friendly Angular wrapper for CodeMirror v6 with a clean, intuitive API.

## Basic Usage

```typescript
import { Codemirror, CodeMirrorConfig } from '@nifi/shared';
import { json } from '@codemirror/lang-json';

@Component({
    template: `
        <codemirror [config]="editorConfig" (ready)="onEditorReady($event)" (contentChange)="onContentChange($event)">
        </codemirror>
    `
})
export class MyComponent {
    editorConfig: CodeMirrorConfig = {
        plugins: [json()], // Add language support
        focusOnInit: true, // Auto-focus when ready
        readOnly: false, // Allow editing
        appearance: myTheme // Custom theme
    };

    onEditorReady(editor: Codemirror) {
        // Now you can use the clean API!
        editor.setEditorValue('{"hello": "world"}');
        editor.selectAll();
    }
}
```

## Developer-Friendly API Methods

### Event Handling

```typescript
editor.addEventListener('keydown', handler); // Add event listener
editor.removeEventListener('keydown', handler); // Remove event listener
```

### Content Management

```typescript
editor.getValue(); // Get current content
editor.setEditorValue('new content'); // Set content
editor.insertText('hello'); // Insert at cursor
editor.replaceRange(0, 5, 'hi'); // Replace text range
```

### Selection & Navigation

```typescript
editor.selectAll(); // Select all text
editor.getSelection(); // Get current selection {from, to}
editor.setSelection(0, 10); // Set selection range
editor.focus(); // Focus the editor
editor.hasFocus(); // Check if focused
```

### Document Information

```typescript
editor.getLineCount(); // Get number of lines
editor.getLine(1); // Get text of specific line (1-based)
```

## Language Support

Add languages by importing and including them in the `plugins` array:

```typescript
import { json } from '@codemirror/lang-json';
import { xml } from '@codemirror/lang-xml';
import { yaml } from '@codemirror/lang-yaml';

const config: CodeMirrorConfig = {
    plugins: [
        json(), // JSON syntax highlighting
        xml(), // XML syntax highlighting
        yaml() // YAML syntax highlighting
        // ... other extensions
    ]
};
```

## Configuration Options

```typescript
interface CodeMirrorConfig {
    appearance?: Extension; // Theme/styling
    focusOnInit?: boolean; // Auto-focus when ready
    content?: string; // Initial content
    disabled?: boolean; // Disable editing
    readOnly?: boolean; // Read-only mode
    plugins?: Extension[]; // Language & other extensions
}
```

## Events

```typescript
<codemirror
  (ready)="onReady($event)"           // Editor ready with API
  (contentChange)="onChange($event)"  // Content changed
  (focused)="onFocus()"              // Editor focused
  (blurred)="onBlur()">              // Editor blurred
</codemirror>
```
