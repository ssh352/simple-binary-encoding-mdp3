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
package uk.co.real_logic.sbe.read_cme_pcaps.token_listeners;

import org.agrona.DirectBuffer;
import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.otf.Types;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CompactTokenListener implements TokenListener {
    static long event_count;
    private final int template_id;
    private final Writer out;
    private final Deque<String> nonTerminalScope = new ArrayDeque<>();
    private final byte[] tempBuffer = new byte[1024];
    CharSequence transact_time;
    long message_count;
    long group_header_count;
    long group_element_count;
    boolean include_value_labels = true;
    boolean transact_time_found;
    boolean print_full_scope;
    private int compositeLevel;
    private final long packet_sequence_number;
    private final long sending_time;
    private TokenOutput tokenOutput;

    public CompactTokenListener(final Writer out, long message_count, long packet_sequence_number, long sending_time, int template_id, boolean include_value_labels) {
        this.template_id = template_id;
        this.sending_time = sending_time;
        this.packet_sequence_number = packet_sequence_number;
        this.message_count = message_count;
        this.out = out;
        this.include_value_labels = include_value_labels;
        this.print_full_scope = true;
        this.tokenOutput = new TokenOutput(out, include_value_labels);
    }


    private static CharSequence readEncodingAsString(
            final DirectBuffer buffer, final int index, final Token typeToken, final int actingVersion) {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue) {
            if (constOrNotPresentValue.size() == 1) {
                final byte[] bytes = {(byte) constOrNotPresentValue.longValue()};
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

    public void onBeginMessage(final Token token) {
        this.group_header_count = 0;
        this.nonTerminalScope.push(token.name());
    }

    public void onEndMessage(final Token token) {
        this.nonTerminalScope.pop();
        this.tokenOutput.writerOut("\n");


        try {
            this.out.flush();
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
        String terminalValueString = terminalValue.toString();
        //transact time is special case.. instead of outputting, we want to stash it to output later
        if (!fieldToken.name().equals("TransactTime")) {
            this.tokenOutput.writeFieldValue(fieldToken.name(), terminalValueString);
        } else {
            if (!this.transact_time_found) {
                this.transact_time = terminalValue;
                this.transact_time_found = true;
                //waiting to get transact time before writing row header
                this.writeNewRow(RowType.messageheader);

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

        this.tokenOutput.writerOut(", ");
        if (value != null) {
            this.tokenOutput.writerOut(value);
        } else
            this.tokenOutput.writerOut("null");
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
                if (this.include_value_labels) {
                    sb.append(tokens.get(i).name() + '=');
                } else {
                    sb.append(", ");
                }
            }
            final long bitPosition = tokens.get(i).encoding().constValue().longValue();
            final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

            sb.append(flag);
        }
//        printTimestampsAndTemplateID();
//        printValue(typeToken, encodedValue);
        this.tokenOutput.writerOut(sb.toString());


        event_count++;
    }

    public void onBeginComposite(
            final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex) {
        ++this.compositeLevel;

        this.nonTerminalScope.push(this.determineName(fieldToken, tokens, fromIndex) + ".");
    }

    public void onEndComposite(final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex) {
        --this.compositeLevel;

        this.nonTerminalScope.pop();
    }

    public void onGroupHeader(final Token token, final int numInGroup) {
        this.tokenOutput.writerOut("\n");
        this.group_element_count = 0;
        this.group_header_count++;
        this.writeNewRow(RowType.groupheader);
        this.tokenOutput.writerOut(token.name());
        if (this.include_value_labels) {
            this.tokenOutput.writerOut(", Group Header : numInGroup=");
        } else {
            this.tokenOutput.writerOut(", ");
        }
        this.tokenOutput.writerOut(Integer.toString(numInGroup));
    }

    public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup) {
        this.tokenOutput.writerOut("\n");
        this.group_element_count++;
        this.nonTerminalScope.push(token.name());
        this.writeNewRow(RowType.group);
    }

    public void onEndGroup(final Token token, final int groupIndex, final int numInGroup) {
        this.nonTerminalScope.pop();
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
                buffer.getBytes(bufferIndex, this.tempBuffer, 0, length);
                value = new String(this.tempBuffer, 0, length, characterEncoding);
            }
        } catch (final UnsupportedEncodingException ex) {
            ex.printStackTrace();
            return;
        }

        this.tokenOutput.writeFieldValue(fieldToken.name(), value);
    }

    private String determineName(
            final Token fieldToken, final List<Token> tokens, final int fromIndex) {
        if (this.compositeLevel > 1) {
            return tokens.get(fromIndex).name();
        } else {
            return fieldToken.name();
        }
    }



    private void writeNewRow(RowType row_type) {
        this.tokenOutput.writeRow(row_type, this.message_count, this.group_header_count, this.group_element_count);
        this.writeTimestamps();
        this.printScope();
    }

    public void writeTimestamps() {
        String packet_sequence_number_string = String.format("%d", this.packet_sequence_number);
        String event_count_string = String.format("%d", event_count);
        this.tokenOutput.writerOut(", " + this.template_id + ", " + packet_sequence_number_string + ", " + event_count_string + ", " + this.sending_time + ", " + this.transact_time);
    }


    private void printScope() {
        this.tokenOutput.writerOut(", ");
        final Iterator<String> i = this.nonTerminalScope.descendingIterator();
        while (i.hasNext()) {
            if (this.print_full_scope | (!i.hasNext())) {
                this.tokenOutput.writerOut(i.next());
            } else {
                i.next();
            }
        }
    }


    enum RowType {
        messageheader, groupheader, group
    }
}
