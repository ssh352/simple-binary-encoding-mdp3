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
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.PacketInfo.PacketInfo;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.CounterTypes;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.decoders.BufferDecoders;

import java.io.UnsupportedEncodingException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class CompactTokenListener implements TokenListener {
    private final Deque<String> nonTerminalScope = new ArrayDeque<>();
    private final byte[] tempBuffer = new byte[1024];
    boolean include_value_labels = true;
    final boolean print_full_scope;
    private int compositeLevel;
    private final TokenOutput tokenOutput;
    private final RowCounter row_counter;
    final private PacketInfo packetInfo;

    public CompactTokenListener(final TokenOutput tokenOutput, RowCounter row_counter, PacketInfo packetInfo,   boolean include_value_labels) {
        this.packetInfo=packetInfo;
        this.include_value_labels = include_value_labels;
        this.print_full_scope = true;
        this.tokenOutput = tokenOutput;
        this.row_counter = row_counter;
    }


    public void onBeginMessage(final Token token) {
        //todo: raise onBeginMessageInRowCounter
        this.row_counter.onBeginMessage();
        this.nonTerminalScope.push(token.name());
    }

    public void onEndMessage(final Token token) {
        this.nonTerminalScope.pop();
        this.tokenOutput.writerOut("\n");


        this.tokenOutput.flush();
        row_counter.increment_count(CounterTypes.MESSAGE_COUNT);

    }

    public void onEncoding(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int index,
            final Token typeToken,
            final int actingVersion) {

        final CharSequence terminalValue = BufferDecoders.readEncodingAsString(buffer, index, typeToken, actingVersion);
        String terminalValueString = terminalValue.toString();
        //transact time is special case.. instead of outputting, we want to stash it to output later
        if (!fieldToken.name().equals("TransactTime")) {
            this.tokenOutput.writeFieldValue(fieldToken.name(), terminalValueString);
        } else {
            if (!this.packetInfo.transactTimeSet()) {
                this.packetInfo.setTransactTime(terminalValueString);
                //waiting to get transact time before writing row header
                this.tokenOutput.writeRowHeader(RowType.messageheader, packetInfo,  makeScopeString());

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
        final long encodedValue = BufferDecoders.readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

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
        final long encodedValue = BufferDecoders.readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        StringBuilder sb = new StringBuilder();
        for (int i = beginIndex + 1; i < endIndex; i++) {
            sb.append(", ");
                if (this.include_value_labels) {
                    sb.append(tokens.get(i).name() + '=');
                }
            final long bitPosition = tokens.get(i).encoding().constValue().longValue();
            final boolean flag = (encodedValue & (1L << bitPosition)) != 0;
            sb.append(flag);
        }
        tokenOutput.writerOut(sb.toString());
        row_counter.onBitSetEnd();
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
        this.row_counter.onGroupHeader();
        this.tokenOutput.writerOut("\n");
        this.tokenOutput.writeRowHeader(RowType.groupheader, packetInfo, makeScopeString());
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
        this.row_counter.onBeginGroup();
        this.nonTerminalScope.push(token.name());
        this.tokenOutput.writeRowHeader(RowType.group, packetInfo, makeScopeString());
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


    private String makeScopeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(", ");
        final Iterator<String> i = this.nonTerminalScope.descendingIterator();
        while (i.hasNext()) {
            if (this.print_full_scope | (!i.hasNext())) {
                sb.append(i.next());
            } else {
                i.next();
            }
        }
        return sb.toString();
    }


    enum RowType {
        messageheader, groupheader, group
    }
}
