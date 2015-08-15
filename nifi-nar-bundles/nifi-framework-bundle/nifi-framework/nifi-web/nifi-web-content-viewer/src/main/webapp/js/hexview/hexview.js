/*
HexViewJS License
-----------------

HexViewJS is written by Nick McVeity <nmcveity@gmail.com> and is 
licensed under the terms of the MIT license reproduced below.

========================================================================

Copyright (c) 2010 Nick McVeity <nmcveity@gmail.com>

Permission is hereby granted, free of charge, to any person 
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be 
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

========================================================================
 */

$(document).ready(function () {
    var HEX = '0123456789ABCDEF';

    function dec2_to_hex(dec)
    {
        if (dec < 0)
            dec = 0;

        if (dec > 255)
            dec = 255;

        return HEX.charAt(Math.floor(dec / 16)) + HEX.charAt(dec % 16);
    }

    function dec_to_hex8(dec)
    {
        var str = "";

        for (var i = 3; i >= 0; i--)
        {
            str += dec2_to_hex((dec >> (i*8)) & 255);
        }

        return str;
    }

    function remove_whitespace(str)
    {
        return str.replace(/\n/g, "")
                  .replace(/\t/g, "")
                  .replace(/ /g, "")
                  .replace(/\r/g, "");
    }

    var BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";

    function base64_decode(encoded)
    {
        var decoded = "";

        for (var i = 0; i < encoded.length; i += 4)
        {
            var ch0 = encoded.charAt(i+0);
            var ch1 = encoded.charAt(i+1);
            var ch2 = encoded.charAt(i+2);
            var ch3 = encoded.charAt(i+3);

            var index0 = BASE64_CHARS.indexOf(ch0);
            var index1 = BASE64_CHARS.indexOf(ch1);
            var index2 = BASE64_CHARS.indexOf(ch2);
            var index3 = BASE64_CHARS.indexOf(ch3);

            decoded += String.fromCharCode((index0 << 2) | (index1 >> 4));
            decoded += String.fromCharCode(((index1 & 15) << 4) | (index2 >> 2));
            
            // skip the base64 padding as those weren't present in the actual bytes
            var token = String.fromCharCode(((index2 & 3) << 6) | index3);
            if (index3 !== 64 || token !== '@') {
                decoded += token;
            }
        }

        return decoded;
    }

    function markup_hexviewwindow(div, index)
    {
        var entityMap = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;',
            '/': '&#x2f;'
        };

        function escapeHtml(string) {
            if (string === null || typeof string === 'undefined') {
                return '';
            } else {
                return String(string).replace(/[&<>"'\/]/g, function (s) {
                    return entityMap[s];
                });
            }
        }

        var bin_data = base64_decode(remove_whitespace(div.text()));
        var line_data;
        var title = div.attr("title");

        var highlights_str = $("form#hexviewwindow_params input[name='highlights']", div).attr("value").split(',');
        var highlights = [];

        for (var i = 0; i < highlights_str.length; i++)
        {
            highlights.push(highlights_str[i].split(":"));
        }

        var params = title.split(':');
        var step = parseInt($("form#hexviewwindow_params input[name='row_width']", div).attr("value"));
        var word_size = parseInt($("form#hexviewwindow_params input[name='word_size']", div).attr("value"));
        var hide_0x = parseInt($("form#hexviewwindow_params input[name='hide_0x']", div).attr("value"));
        var decimal_offset = parseInt($("form#hexviewwindow_params input[name='decimal_offset']", div).attr("value"));
        var start_byte_1 = parseInt($("form#hexviewwindow_params input[name='start_byte_1']", div).attr("value"));
        var caption = $("form#hexviewwindow_params input[name='caption']", div).attr("value");

        div.text("");
        div.append("<table></table>");

        var offset = (start_byte_1 ? 1 : 0);

        function apply_highlights(index)
        {
            for (var j = 0; j < highlights.length; j++)
            {
                if ((index >= highlights[j][0]) && (index <= highlights[j][1]))
                {
                    if (index === highlights[j][0])
                    {
                        $("table tr td:last", div).addClass("hexviewerwindow_border_start");
                    }

                    if (index === highlights[j][1])
                    {
                        $("table tr td:last", div).addClass("hexviewerwindow_border_end");
                    }

                    $("table tr td:last", div).addClass("hexviewerwindow_code_hi hexviewerwindow_border_middle");
                    $("table tr td:last", div).attr("style", "background-color: " + highlights[j][2] + ";");
                    $("table tr td:last", div).attr("title", highlights[j][3]);

                    runlen += 1;
                }
                else
                {
                    $("table tr td:last", div).addClass("hexviewerwindow_code");
                }
            }
        }

        if (caption)
            $("table", div).append("<caption>" + escapeHtml(caption) + "</caption>");

        while (bin_data.length > 0)
        {
            line_data = bin_data.slice(0, step);
            bin_data = bin_data.slice(step);

            $("table", div).addClass("hexviewerwindow_table");
            $("table", div).append("<tr></tr>").addClass("hexviewerwindow");
            $("table tr:last", div).append("<td>" + escapeHtml((decimal_offset ? ("00000000"+offset).slice(-8) : "0x" + dec_to_hex8(offset))) + "</td>");
            $("table tr td:last", div).addClass("hexviewerwindow_offset");

            var runlen = 0;

            for (var i = 0; i < line_data.length; i += word_size)
            {
                var num = "";

                for (var j = 0; j < word_size; j++)
                {
                    num += dec2_to_hex(line_data.charCodeAt(i+j));
                }

                $("table tr:last", div).append("<td>" + escapeHtml((hide_0x ? "" : "0x") + num) + "</td>");

                apply_highlights(offset+i);
            }

            var text = "";

            for (var i = 0; i < line_data.length; i++)
            {
                var cc = line_data.charCodeAt(i);

                if ((cc >= 32) && (cc <= 126))
                {
                    text = text + line_data.charAt(i);
                }
                else
                {
                    text = text + ".";
                }
            }

            if (line_data.length < step)
                $("table tr td:last", div).attr("colspan", Math.floor((step - line_data.length) / word_size) + 1);

            offset += step;

            $("table tr:last", div).append("<td>" + escapeHtml(text) + "</td>");
            $("table tr td:last", div).addClass("hexviewerwindow_visual");
        }
    }

    $("div.hexviewwindow").each(function (index) {
        markup_hexviewwindow($(this), index);
    });
});