/*
 * Copyright 2013-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.sbe.read_cme_pcaps;

import org.agrona.DirectBuffer;
import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.otf.Types;

import java.io.IOException;
import java.io.Writer;
import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.*;

public class CompactTokenListener implements TokenListener {
    private final int template_id;
    private int compositeLevel = 0;
    private final Writer out;
    private final Deque<String> nonTerminalScope = new ArrayDeque<>();
    private final byte[] tempBuffer = new byte[1024];
    private long sending_time;
    private long message_count;
    CharSequence transact_time = null;
    long message_index;
    long row_type_index = 0;
    long type_entry_index = 0;
    boolean include_value_labels = true;
    boolean transact_time_found = false;
    boolean print_full_scope;

    enum RowType {
        messageheader, groupheader, group
    }


    public CompactTokenListener(final Writer out, long message_index, long sending_time, int template_id, boolean include_value_labels) {
        this.template_id = template_id;
        this.message_index = message_index;
        this.sending_time = sending_time;
        this.out = out;
        this.include_value_labels = include_value_labels;
        this.print_full_scope = true;
    }



    public void onBeginMessage(final Token token) {
        row_type_index=0;
        nonTerminalScope.push(token.name());
    }

    public void onEndMessage(final Token token) {
        nonTerminalScope.pop();
        try {
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void onEncoding(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int index,
            final Token typeToken,
            final int actingVersion) {

        final CharSequence terminalValue = readEncodingAsString(buffer, index, typeToken, actingVersion);

        //transact time is special case.. instead of outputting, we want to stash it to output later
        if (!fieldToken.name().equals("TransactTime")) {
            printValue(fieldToken, terminalValue);
        } else {
            if(!transact_time_found){
                this.transact_time = terminalValue;
                this.transact_time_found = true;
            }
        }

    }

    public void onEnum(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int bufferIndex,
            final List<Token> tokens,
            final int beginIndex,
            final int endIndex,
            final int actingVersion) {
        final Token typeToken = tokens.get(beginIndex + 1);
        final long encodedValue = readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        String value = null;
        if (fieldToken.isConstantEncoding()) {
            final String refValue = fieldToken.encoding().constValue().toString();
            final int indexOfDot = refValue.indexOf('.');
            value = -1 == indexOfDot ? refValue : refValue.substring(indexOfDot + 1);
        } else {
            for (int i = beginIndex + 1; i < endIndex; i++) {
                if (encodedValue == tokens.get(i).encoding().constValue().longValue()) {
                    value = tokens.get(i).name();
                    break;
                }
            }
        }

//        printValue(typeToken, value);
//        printValue(determineName(0, fieldToken, tokens, beginIndex), value);
    }

    public void onBitSet(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int bufferIndex,
            final List<Token> tokens,
            final int beginIndex,
            final int endIndex,
            final int actingVersion) {
        final Token typeToken = tokens.get(beginIndex + 1);

        final long encodedValue = readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        //     toWriter(determineName(0, fieldToken, tokens, beginIndex)).append(':');

        //hold value of string for a bit, so we can get the transact time from the header
        StringBuilder sb = new StringBuilder();
        for (int i = beginIndex + 1; i < endIndex; i++) {
            //don't display transact time.. it is special case on every row
            if (!tokens.get(i).name().equals("TransactTime")) {
                if (include_value_labels) {
                    sb.append(tokens.get(i).name() + '=');
                } else {
                    sb.append(", ");
                }
            }
            final long bitPosition = tokens.get(i).encoding().constValue().longValue();
            final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

            sb.append(Boolean.toString(flag));
        }
//        printTimestampsAndTemplateID();
        writeNewRow(RowType.messageheader);
//        printValue(typeToken, encodedValue);
        writerOut(sb.toString());
        writerOut("\n");
        row_type_index++;
    }

    public void onBeginComposite(
            final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex) {
        ++compositeLevel;

        nonTerminalScope.push(determineName(1, fieldToken, tokens, fromIndex) + ".");
    }

    public void onEndComposite(final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex) {
        --compositeLevel;

        nonTerminalScope.pop();
    }

    public void onGroupHeader(final Token token, final int numInGroup) {
        type_entry_index = 0;
        writeNewRow(RowType.groupheader);
        writerOut(token.name());
        if (include_value_labels) {
            writerOut(", Group Header : numInGroup=");
        } else {
            writerOut(", ");
        }
        writerOut(Integer.toString(numInGroup));
        writerOut("\n");
        row_type_index++;
    }

    public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup) {
        nonTerminalScope.push( token.name());
        writeNewRow(RowType.group);
    }

    public void onEndGroup(final Token token, final int groupIndex, final int numInGroup) {
        nonTerminalScope.pop();
        type_entry_index++;
        writerOut("\n");
    }

    public void onVarData(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int bufferIndex,
            final int length,
            final Token typeToken) {
        final String value;
        try {
            final String characterEncoding = typeToken.encoding().characterEncoding();
            if (null == characterEncoding) {
                value = length + " bytes of raw data";
            } else {
                buffer.getBytes(bufferIndex, tempBuffer, 0, length);
                value = new String(tempBuffer, 0, length, characterEncoding);
            }
        } catch (final UnsupportedEncodingException ex) {
            ex.printStackTrace();
            return;
        }

        printValue(fieldToken, value);
    }

    private String determineName(
            final int thresholdLevel, final Token fieldToken, final List<Token> tokens, final int fromIndex) {
        if (compositeLevel > thresholdLevel) {
            return tokens.get(fromIndex).name();
        } else {
            return fieldToken.name();
        }
    }

    private static CharSequence readEncodingAsString(
            final DirectBuffer buffer, final int index, final Token typeToken, final int actingVersion) {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue) {
            if (constOrNotPresentValue.size() == 1) {
                final byte[] bytes = {(byte) constOrNotPresentValue.longValue()};
                //            System.toWriterln((bytes[0]));
                //           System.toWriterln((constOrNotPresentValue.characterEncoding()));
                return new String(bytes, UTF_8);

            } else {
                return constOrNotPresentValue.toString();
            }
        }

        final StringBuilder sb = new StringBuilder();
        final Encoding encoding = typeToken.encoding();
        final int elementSize = encoding.primitiveType().size();

        for (int i = 0, size = typeToken.arrayLength(); i < size; i++) {
            Types.appendAsString(sb, buffer, index + (i * elementSize), encoding);
            sb.append(", ");
        }

        sb.setLength(sb.length() - 2);

        return sb;
    }

    private static long readEncodingAsLong(
            final DirectBuffer buffer, final int bufferIndex, final Token typeToken, final int actingVersion) {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue) {
            return constOrNotPresentValue.longValue();
        }

        return Types.getLong(buffer, bufferIndex, typeToken.encoding());
    }

    private static PrimitiveValue constOrNotPresentValue(final Token token, final int actingVersion) {
        if (token.isConstantEncoding()) {
            return token.encoding().constValue();
        } else if (token.isOptionalEncoding() && actingVersion < token.version()) {
            return token.encoding().applicableNullValue();
        }

        return null;
    }

    private void printValue(Token typeToken, Object printableObject) {
        String field_label = typeToken.name();
        printValue(field_label, printableObject);
    }

    private void printValue(String field_label, Object printableObject) {
        writerOut(", ");
        if (include_value_labels) {
            writerOut(field_label);
            writerOut("=");
        }
        writerOut(printableObject);
        //here is where it prints the deep scope for each value.. we'd like to somehow
    }

    private void writeNewRow(RowType row_type) {
        writeRowHeader(row_type);
        writeTimestamps();
        printScope();
    }

    public void writeTimestamps() {
        writerOut(", " + template_id + ", " + sending_time + ", " + transact_time );
    }

    private void writeRowHeader(RowType row_type) {
        writerOut(message_index);
        writerOut(", ");
        writerOut(row_type_index);
        writerOut(", ");
        writerOut(type_entry_index);
        writerOut(", ");
        writerOut(pad(row_type.toString(), 13, ' '));
    }

    private void printScope() {
        writerOut(", ");
        final Iterator<String> i = nonTerminalScope.descendingIterator();
        while (i.hasNext()) {
            if (print_full_scope | (!i.hasNext())) {
                writerOut(i.next());
            } else {
                i.next();
            }
        }
    }

    private void writerOut(Object o) {
        String s = o.toString();
        writerOut(s);
    }

    private void writerOut(String s) {
        try {
            out.write(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String pad(String str, int size, char padChar)
    {
        StringBuffer padded = new StringBuffer(str);
        while (padded.length() < size)
        {
            padded.append(padChar);
        }
        return padded.toString();
    }
}
