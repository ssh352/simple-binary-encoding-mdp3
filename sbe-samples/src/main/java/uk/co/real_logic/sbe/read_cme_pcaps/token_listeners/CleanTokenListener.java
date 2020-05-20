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
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.ScopeTracker;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;


public class CleanTokenListener implements TokenListener {
    private int compositeLevel;
    private final TablesHandler tablesHandler;
    //todo: possibly change namedscope to hold names without dots
    //todo: possible explicitly track lecel of depth/type of table
    private final byte[] tempBuffer = new byte[1024];

    public CleanTokenListener(TablesHandler tablesHandler) {
        //todo: take tableshandler as additional input
        this.tablesHandler =tablesHandler;
    }

    public void onBeginMessage(final Token token) {
        //todo: put name of template into messageheaders tableaa
        this.tablesHandler.startMessageHeader(token.name());


    }

    public void onEndMessage(final Token token) {
    }

    public void onEncoding(
            final Token fieldToken,
            final DirectBuffer buffer,
            final int index,
            final Token typeToken,
            final int actingVersion) {
        final CharSequence value = readEncodingAsString(buffer, index, typeToken, actingVersion);

        this.tablesHandler.appendColumnValue(this.compositeLevel > 0 ? typeToken.name() : fieldToken.name(), String.valueOf(value));
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

        this.tablesHandler.appendColumnValue(this.determineName(0, fieldToken, tokens, beginIndex), value);
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

//        this.tablesHandler.appendScope();
//        this.tablesHandler.appendToCurrentScope(this.determineName(0, fieldToken, tokens, beginIndex));
//        this.tablesHandler.appendToCurrentScope(":");

        for (int i = beginIndex + 1; i < endIndex; i++) {

            final long bitPosition = tokens.get(i).encoding().constValue().longValue();
            final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

            this.tablesHandler.appendColumnValue(tokens.get(i).name(), Boolean.toString(flag));
        }
    }

    public void onBeginComposite(
            final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex) {
        ++this.compositeLevel;

        this.tablesHandler.pushScope(this.determineName(1, fieldToken, tokens, fromIndex));
    }

    public void onEndComposite(final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex) {
        --this.compositeLevel;
        this.tablesHandler.popScope();
    }

    public void onGroupHeader(final Token token, final int numInGroup) throws IOException {
        this.tablesHandler.endMessageHeader(token.name());
        this.tablesHandler.beginGroupHeader();
        //todo: write all values of group header table
        this.tablesHandler.appendColumnValue( "groupheadername", token.name());
//        this.tablesHandler.appendToResidual(token.name());
        this.tablesHandler.appendColumnValue("numInGroup",Integer.toString(numInGroup));
        this.tablesHandler.endGroupHeader();
    }

    public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup) throws IOException {
        this.tablesHandler.beginGroup(token.name());
    }

    public void onEndGroup(final Token token, final int groupIndex, final int numInGroup) throws IOException {
        this.tablesHandler.endGroup();
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

        this.tablesHandler.appendColumnValue(fieldToken.name(), value);
    }

    @Override
    public void writeString(String output) {

    }



    private String determineName(
            final int thresholdLevel, final Token fieldToken, final List<Token> tokens, final int fromIndex) {
        if (this.compositeLevel > thresholdLevel) {
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
                    return new String(bytes,UTF_8);
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

}
