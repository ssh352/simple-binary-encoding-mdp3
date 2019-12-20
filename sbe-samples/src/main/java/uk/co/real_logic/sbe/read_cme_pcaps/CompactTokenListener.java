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

import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.*;

public class CompactTokenListener implements TokenListener {
    private int compositeLevel = 0;
    private final PrintWriter out;
    private final Deque<String> nonTerminalScope = new ArrayDeque<>();
    private final byte[] tempBuffer = new byte[1024];
    private long sending_time;
    private long message_count;
    CharSequence transact_time = null;
    long message_index;
    long group_type_index = 0;
    long group_entry_index = 0;
    boolean include_value_labels = true;
    boolean transact_time_found = false;
    boolean is_first_token = true;

    enum RowType {
        messageheader, groupheader, group
    }

    public CompactTokenListener(final PrintWriter out) {
        this.out = out;
    }

    public CompactTokenListener(final PrintWriter out, long message_index, long sending_time, boolean include_value_labels) {
        this.message_index = message_index;
        this.sending_time = sending_time;
        this.out = out;
        this.include_value_labels = include_value_labels;
    }


    public void printTimestamps() {
        out.print(", " + sending_time + ", " + transact_time);
    }

    public void onBeginMessage(final Token token) {
        nonTerminalScope.push(token.name() + ".");
    }

    public void onEndMessage(final Token token) {
        nonTerminalScope.pop();
    }

    public void onEncoding(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int index,
            final Token typeToken,
            final int actingVersion) {

        final CharSequence terminalValue = readEncodingAsString(buffer, index, typeToken, actingVersion);
        if (!transact_time_found & fieldToken.name().equals("TransactTime")) {
            this.transact_time = terminalValue;
        }
        printValue(fieldToken, terminalValue);
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

        printValue(typeToken, value);
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

        printValue(typeToken, encodedValue);
   //     out.append(determineName(0, fieldToken, tokens, beginIndex)).append(':');

        for (int i = beginIndex + 1; i < endIndex; i++) {
            out.append(' ').append(tokens.get(i).name()).append('=');

            final long bitPosition = tokens.get(i).encoding().constValue().longValue();
            final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

            out.append(Boolean.toString(flag));
        }

        out.println();
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
        group_entry_index=0;
        printNewRow(RowType.groupheader);
        out.append(token.name())
                .append(" Group Header : numInGroup=")
                .append(Integer.toString(numInGroup))
                .println();
        group_type_index++;
    }

    public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup) {
        nonTerminalScope.push(token.name() + ".");
        printNewRow(RowType.group);
    }

    public void onEndGroup(final Token token, final int groupIndex, final int numInGroup) {
        nonTerminalScope.pop();
        group_entry_index++;
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
                //            System.out.println((bytes[0]));
                //           System.out.println((constOrNotPresentValue.characterEncoding()));
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
        String field_label=typeToken.name();
        printValue(field_label, printableObject);
    }

    private void printValue(String field_label, Object printableObject) {
        out.print(", ");
        if (include_value_labels) {
            out.print(field_label);
            out.print("=");
        }
        out.print(printableObject);
        //here is where it prints the deep scope for each value.. we'd like to somehow
    }

    private void printNewRow(RowType row_type) {
        out.print(message_index);
        printTimestamps();
        printRowHeader(row_type);
        printScope();
    }

    private void printRowHeader(RowType row_type){
        out.print(", " + row_type + ", ");
        if (row_type == RowType.group) {
            out.print(group_type_index + ", " + group_type_index);
            out.print(group_entry_index + ", " + group_entry_index);
        }

    }

    private void printScope(){
        final Iterator<String> i = nonTerminalScope.descendingIterator();
        {
            while (i.hasNext()) {
                out.print(i.next());
            }
        }
    }
}
