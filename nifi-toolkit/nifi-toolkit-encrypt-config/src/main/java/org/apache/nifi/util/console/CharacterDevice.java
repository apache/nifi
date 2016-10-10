/*
Copyright (c) 2010 McDowell

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */
package org.apache.nifi.util.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;

/**
 * @{link TextDevice} implementation wrapping character streams.
 *
 * @author McDowell
 */
class CharacterDevice extends TextDevice {
    private final BufferedReader reader;
    private final PrintWriter writer;

    public CharacterDevice(BufferedReader reader, PrintWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public CharacterDevice printf(String fmt, Object... params)
            throws ConsoleException {
        writer.printf(fmt, params);
        return this;
    }

    @Override
    public String readLine() throws ConsoleException {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new IllegalStateException();
        }
    }

    @Override
    public char[] readPassword() throws ConsoleException {
        return readLine().toCharArray();
    }

    @Override
    public Reader reader() throws ConsoleException {
        return reader;
    }

    @Override
    public PrintWriter writer() throws ConsoleException {
        return writer;
    }
}