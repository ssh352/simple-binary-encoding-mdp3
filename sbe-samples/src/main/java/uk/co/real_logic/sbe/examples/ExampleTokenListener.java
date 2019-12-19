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
package uk.co.real_logic.sbe.examples;

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
public class ExampleTokenListener implements TokenListener
{
    private int compositeLevel = 0;
    private final PrintWriter out;
    private final Deque<String> namedScope = new ArrayDeque<>();
    private final byte[] tempBuffer = new byte[1024];
    private boolean verbose;
    private long sending_time;
    CharSequence transact_time;

    public ExampleTokenListener(final PrintWriter out)
    {
        this.verbose=true;
        this.out = out;
    }


    public ExampleTokenListener(final PrintWriter out, boolean verbose, long sending_time)
    {
        this.sending_time=sending_time;
        this.verbose=verbose;
        this.out = out;
    }

    public void PrintSendingTime(){
        out.print("sending_time: " + sending_time + ", transact_time:" + transact_time + ", ");
    }

    public void onBeginMessage(final Token token)
    {
        out.print(" message_token_name=" + token.name());
        namedScope.push(token.name() + ".");
    }

    public void onEndMessage(final Token token)
    {
        namedScope.pop();
    }

    public void onEncoding(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int index,
        final Token typeToken,
        final int actingVersion)
    {
       // System.out.println("type token: " +  typeToken);
        if(fieldToken.name().contains("Size")) {
            int abc=1;
        }
        final CharSequence value = readEncodingAsString(buffer, index, typeToken, actingVersion, sending_time);
        if(fieldToken.name().equals("TransactTime")){
            this.transact_time=value;
        }

        printScope();
        if(verbose) {
            out.append(compositeLevel > 0 ? typeToken.name() : fieldToken.name())
                    .append('=')
                    .append(value)
                    .println();
        }else{
            out.append(compositeLevel > 0 ? typeToken.name() : fieldToken.name())
                    .append('=')
                    .append(value)
                    .append(" ");
        }
    }

    public void onEnum(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferIndex,
        final List<Token> tokens,
        final int beginIndex,
        final int endIndex,
        final int actingVersion)
    {
        final Token typeToken = tokens.get(beginIndex + 1);
        final long encodedValue = readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        String value = null;
        if (fieldToken.isConstantEncoding())
        {
            final String refValue = fieldToken.encoding().constValue().toString();
            final int indexOfDot = refValue.indexOf('.');
            value = -1 == indexOfDot ? refValue : refValue.substring(indexOfDot + 1);
        }
        else
        {
            for (int i = beginIndex + 1; i < endIndex; i++)
            {
                if (encodedValue == tokens.get(i).encoding().constValue().longValue())
                {
                    value = tokens.get(i).name() + " k";
                    break;
                }
            }
        }

        printScope();
        out.append(determineName(0, fieldToken, tokens, beginIndex))
            .append(" h =")
            .append(value)
            .println();
    }

    public void onBitSet(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferIndex,
        final List<Token> tokens,
        final int beginIndex,
        final int endIndex,
        final int actingVersion)
    {
        final Token typeToken = tokens.get(beginIndex + 1);
        final long encodedValue = readEncodingAsLong(buffer, bufferIndex, typeToken, actingVersion);

        printScope();
        out.append(determineName(0, fieldToken, tokens, beginIndex)).append(" i :");
        if(verbose) {
            for (int i = beginIndex + 1; i < endIndex; i++) {
                out.append(' ').append(tokens.get(i).name()).append(" j =");

                final long bitPosition = tokens.get(i).encoding().constValue().longValue();
                final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

                out.append(Boolean.toString(flag));
            }

            out.println();
        } else{
// group header?
                for (int i = beginIndex + 1; i < endIndex; i++) {
                    out.append(tokens.get(i).name());

                    final long bitPosition = tokens.get(i).encoding().constValue().longValue();
                    final boolean flag = (encodedValue & (1L << bitPosition)) != 0;

                    out.append(Boolean.toString(flag));
                }
            }
    }

    public void onBeginComposite(
        final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex)
    {
        ++compositeLevel;

        namedScope.push(determineName(1, fieldToken, tokens, fromIndex));
    }

    public void onEndComposite(final Token fieldToken, final List<Token> tokens, final int fromIndex, final int toIndex)
    {
        --compositeLevel;

        namedScope.pop();
    }

    public void onGroupHeader(final Token token, final int numInGroup)
    {
        printScope();
        out.append(token.name())
            .append("\n e Group Header : numInGroup=")
            .append(Integer.toString(numInGroup))
            .println();
    }

    public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup)
    {
        if(groupIndex==0) {
            PrintSendingTime();
        }
        if(token.name().contains("EntrySize" )){
            namedScope.push("");
        }
        out.print(" group_token_name=" + token.name() + " ");
        namedScope.push(token.name() + ".m." );
    }

    public void onEndGroup(final Token token, final int groupIndex, final int numInGroup)
    {
        namedScope.pop();
    }

    public void onVarData(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferIndex,
        final int length,
        final Token typeToken)
    {
        final String value;
        try
        {
            final String characterEncoding = typeToken.encoding().characterEncoding();
            if (null == characterEncoding)
            {
                value = length + " bytes of raw data";
            }
            else
            {
                buffer.getBytes(bufferIndex, tempBuffer, 0, length);
                value = new String(tempBuffer, 0, length, characterEncoding);
            }
        }
        catch (final UnsupportedEncodingException ex)
        {
            ex.printStackTrace();
            return;
        }

        printScope();
        if(verbose){
        out.append(fieldToken.name())
            .append('=')
            .append(value)
            .println();}
        else{
            out.append(fieldToken.name()).append('=').append(value);

        }
    }

    private String determineName(
        final int thresholdLevel, final Token fieldToken, final List<Token> tokens, final int fromIndex)
    {
        if (compositeLevel > thresholdLevel)
        {
            return tokens.get(fromIndex).name();
        }
        else
        {
            return fieldToken.name();
        }
    }

    private static CharSequence readEncodingAsString(
        final DirectBuffer buffer, final int index, final Token typeToken, final int actingVersion, long sending_time)  //added sending time as argument for printing
    {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue)
        {
            if (constOrNotPresentValue.size() == 1)
            {
                try
                {
                    final byte[] bytes = { (byte)constOrNotPresentValue.longValue() };
                    System.out.println("charset_name:"+  constOrNotPresentValue.characterEncoding());
                    return new String(bytes, "UTF-8"); //default charset to UTF-8  todo: find out why charset is not present
                    }
                catch (final UnsupportedEncodingException ex)
                {
                    throw new RuntimeException(ex);
                }
            }
            else
            {
                return constOrNotPresentValue.toString();
            }
        }

        final StringBuilder sb = new StringBuilder();
        final Encoding encoding = typeToken.encoding();
        final int elementSize = encoding.primitiveType().size();

        for (int i = 0, size = typeToken.arrayLength(); i < size; i++)
        {
//            sb.append(" f ");
            Types.appendAsString(sb, buffer, index + (i * elementSize), encoding);
            sb.append(",");
        }

        sb.setLength(sb.length() - 2);

        return sb;
    }

    private static long readEncodingAsLong(
        final DirectBuffer buffer, final int bufferIndex, final Token typeToken, final int actingVersion)
    {
        final PrimitiveValue constOrNotPresentValue = constOrNotPresentValue(typeToken, actingVersion);
        if (null != constOrNotPresentValue)
        {
            return constOrNotPresentValue.longValue();
        }

        return Types.getLong(buffer, bufferIndex, typeToken.encoding());
    }

    private static PrimitiveValue constOrNotPresentValue(final Token token, final int actingVersion)
    {
        if (token.isConstantEncoding())
        {
            return token.encoding().constValue();
        }
        else if (token.isOptionalEncoding() && actingVersion < token.version())
        {
            return token.encoding().applicableNullValue();
        }

        return null;
    }

    private void printScope()
    {
        //position500 when printing actually happens, perhaps where to get rid of labels?
        final Iterator<String> i = namedScope.descendingIterator();
        while (i.hasNext())
        {
            out.print(i.next());
        }
    }
}
