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
package uk.co.real_logic.sbe.otf;

import org.agrona.DirectBuffer;
import uk.co.real_logic.sbe.ir.Token;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static uk.co.real_logic.sbe.ir.Signal.BEGIN_FIELD;
import static uk.co.real_logic.sbe.ir.Signal.BEGIN_GROUP;
import static uk.co.real_logic.sbe.ir.Signal.BEGIN_VAR_DATA;

/**
 * On-the-fly decoder that dynamically decodes messages based on the IR for a schema.
 * <p>
 * The contents of the messages are structurally decomposed and passed to a {@link TokenListener} for decoding the
 * primitive values.
 * <p>
 * The design keeps all state on the stack to maximise performance and avoid object allocation. The message decoder can
 * be reused repeatably by calling {@link OtfMessageDecoder#decode(DirectBuffer, int, int, int, List, TokenListener)}
 * which is thread safe to be used across multiple threads.
 */
@SuppressWarnings("FinalParameters")
public class OtfMessageDecoder
{
    /**
     * Decode a message from the provided buffer based on the message schema described with IR {@link Token}s.
     *
     * @param buffer        containing the encoded message.
     * @param offset        at which the message encoding starts in the buffer.
     * @param actingVersion of the encoded message for dealing with extension fields.
     * @param blockLength   of the root message fields.
     * @param msgTokens     in IR format describing the message structure.
     * @param listener      to callback for decoding the primitive values as discovered in the structure.
     * @return the index in the underlying buffer after decoding.
     */


    public static int decode(

        final DirectBuffer buffer,
        final int offset,
        final int actingVersion,
        final int blockLength,
        final List<Token> msgTokens,
        final TokenListener listener) throws IOException {
        //do whatever you do when beginning message.. at the least push message name into
        //top of scope. all messages share same table, labeled with (?) as the main key
        listener.onBeginMessage(msgTokens.get(0));

        int i = offset;
        final int numTokens = msgTokens.size();
        //go through fields of header.. not sure how the number is determined. This is a table that
        // is separate for each message type.
        final int tokenIdx = decodeFields(buffer, i, actingVersion, msgTokens, 1, numTokens, listener);
        i += blockLength;

        final long packedValues = decodeGroups(
            buffer, i, actingVersion, msgTokens, tokenIdx, numTokens, listener);

        i = decodeData(
            buffer,
            bufferOffset(packedValues), msgTokens, tokenIndex(packedValues), numTokens,
            actingVersion,
            listener);

        listener.onEndMessage(msgTokens.get(numTokens - 1));

        return i;
    }

    private static int decodeFields(
        final DirectBuffer buffer,
        final int bufferOffset,
        final int actingVersion,
        final List<Token> tokens,
        final int tokenIndex,
        final int numTokens,
        final TokenListener listener) throws IOException {

        int i = tokenIndex;


        //this is (not) the number of fields in the current decode.. it is a global max. The break comes if
        //the field name after the jump is not a begin field
        while (i < numTokens)
        //loopthrough tokens.
        {
            listener.writeString("writing from decode fields ");
            listener.writeString("i=" + i + " ");
            listener.writeString("buffer offset " + bufferOffset + " ");
            listener.writeString("\n");
            final Token fieldToken = tokens.get(i);
            // read all fields in the current scope
            if (BEGIN_FIELD != fieldToken.signal())
            {
                break;
            }
            //make next field index from number of tokens in field (this is in begin fields
            final int nextFieldIdx = i + fieldToken.componentTokenCount();
            //advance to first token composing field.. could be multiple types
            i++;

            //get the type (eg enum, composite, etc.. as well as the initial offest.
            final Token typeToken = tokens.get(i);
            final int offset = typeToken.offset();

            switch (typeToken.signal())
            {
                //all of these are things that can be in a field. probably want to use these to convert
                //values to name/type/value tuples.. but perhaps not to do anything about changing tables
                case BEGIN_COMPOSITE:
                    decodeComposite(
                        fieldToken,
                        buffer,
                        bufferOffset + offset,
                        tokens, i,
                        nextFieldIdx - 2,
                        actingVersion,
                        listener);
                    break;

                case BEGIN_ENUM:
                    listener.onEnum(
                        fieldToken, buffer, bufferOffset + offset, tokens, i, nextFieldIdx - 2, actingVersion);
                    break;

                case BEGIN_SET:
                    listener.onBitSet(
                        fieldToken, buffer, bufferOffset + offset, tokens, i, nextFieldIdx - 2, actingVersion);
                    break;

                case ENCODING:
                    try {
                        listener.onEncoding(fieldToken, buffer, bufferOffset + offset, typeToken, actingVersion);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
            }
            //jump ahead to next thing that needs to be decoded
            i = nextFieldIdx;
        }
        //return index of where the next thing should start
        //possibly this should be used in decode packet..
        return i;
    }

    //includes both group header and grop elements
    private static long decodeGroups(
        final DirectBuffer buffer,
        int bufferOffset,
        final int actingVersion,
        final List<Token> tokens,
        int tokenIdx,
        final int numTokens,
        final TokenListener listener) throws IOException {
        while (tokenIdx < numTokens)
        {
            listener.writeString("writing from decode groups ");
            listener.writeString("tokenidx=" + tokenIdx + " ");
            listener.writeString("buffer offset " + bufferOffset + " ");
            listener.writeString("\n");
            final Token token = tokens.get(tokenIdx);
            //only want to process starting at beginning of group
            //possibly these fields pop up and down from headers, to group headers, to group elements
            if (BEGIN_GROUP != token.signal())
            {
                break;
            }

            final boolean isPresent = token.version() <= actingVersion;

            final Token blockLengthToken = tokens.get(tokenIdx + 2);
            final int blockLength = isPresent ? Types.getInt(
                buffer,
                bufferOffset + blockLengthToken.offset(),
                blockLengthToken.encoding().primitiveType(),
                blockLengthToken.encoding().byteOrder()) : 0;

            final Token numInGroupToken = tokens.get(tokenIdx + 3);
            //todo: write this numInGroup into groupheader table, here or elsewhere
            final int numInGroup = isPresent ? Types.getInt(
                buffer,
                bufferOffset + numInGroupToken.offset(),
                numInGroupToken.encoding().primitiveType(),
                numInGroupToken.encoding().byteOrder()) : 0;

            final Token dimensionTypeComposite = tokens.get(tokenIdx + 1);

            if (isPresent)
            {
                bufferOffset += dimensionTypeComposite.encodedLength();
            }

            final int beginFieldsIdx = tokenIdx + dimensionTypeComposite.componentTokenCount() + 1;

            // basically to advance sciop of group header.. name of group header, number of tokens
            // thus suggests all group headers can be in one table.
            listener.onGroupHeader(token, numInGroup);

            for (int i = 0; i < numInGroup; i++)
            {
                listener.onBeginGroup(token, i, numInGroup);

                final int afterFieldsIdx = decodeFields(
                    buffer, bufferOffset, actingVersion, tokens, beginFieldsIdx, numTokens, listener);
                bufferOffset += blockLength;

                final long packedValues = decodeGroups(
                    buffer, bufferOffset, actingVersion, tokens, afterFieldsIdx, numTokens, listener);

                bufferOffset = decodeData(
                    buffer,
                    bufferOffset(packedValues),
                    tokens,
                    tokenIndex(packedValues),
                    numTokens,
                    actingVersion,
                    listener);

                listener.onEndGroup(token, i, numInGroup);
            }

            tokenIdx += token.componentTokenCount();
        }

        return pack(bufferOffset, tokenIdx);
    }

    private static void decodeComposite(
        final Token fieldToken,
        final DirectBuffer buffer,
        final int bufferOffset,
        final List<Token> tokens,
        final int tokenIdx,
        final int toIndex,
        final int actingVersion,
        final TokenListener listener) throws IOException {
        listener.onBeginComposite(fieldToken, tokens, tokenIdx, toIndex);

        for (int i = tokenIdx + 1; i < toIndex; )
        {
            final Token typeToken = tokens.get(i);
            final int nextFieldIdx = i + typeToken.componentTokenCount();

            final int offset = typeToken.offset();

            switch (typeToken.signal())
            {
                case BEGIN_COMPOSITE:
                    decodeComposite(
                        fieldToken,
                        buffer,
                        bufferOffset + offset,
                        tokens, i,
                        nextFieldIdx - 1,
                        actingVersion,
                        listener);
                    break;

                case BEGIN_ENUM:
                    listener.onEnum(
                        fieldToken, buffer, bufferOffset + offset, tokens, i, nextFieldIdx - 1, actingVersion);
                    break;

                case BEGIN_SET:
                    listener.onBitSet(
                        fieldToken, buffer, bufferOffset + offset, tokens, i, nextFieldIdx - 1, actingVersion);
                    break;

                case ENCODING:
                    listener.onEncoding(typeToken, buffer, bufferOffset + offset, typeToken, actingVersion);
                    break;
            }

            i += typeToken.componentTokenCount();
        }

        listener.onEndComposite(fieldToken, tokens, tokenIdx, toIndex);
    }

    private static int decodeData(
        final DirectBuffer buffer,
        int bufferOffset,
        final List<Token> tokens,
        int tokenIdx,
        final int numTokens,
        final int actingVersion,
        final TokenListener listener) throws IOException {
        while (tokenIdx < numTokens)
        {
            final Token token = tokens.get(tokenIdx);
            if (BEGIN_VAR_DATA != token.signal())
            {
                break;
            }

            final boolean isPresent = token.version() <= actingVersion;

            final Token lengthToken = tokens.get(tokenIdx + 2);
            final int length = isPresent ? Types.getInt(
                buffer,
                bufferOffset + lengthToken.offset(),
                lengthToken.encoding().primitiveType(),
                lengthToken.encoding().byteOrder()) : 0;

            final Token dataToken = tokens.get(tokenIdx + 3);
            if (isPresent)
            {
                bufferOffset += dataToken.offset();
            }

            listener.onVarData(token, buffer, bufferOffset, length, dataToken);

            bufferOffset += length;
            tokenIdx += token.componentTokenCount();
        }

        return bufferOffset;
    }

    private static long pack(final int bufferOffset, final int tokenIndex)
    {
        return ((long)bufferOffset << 32) | tokenIndex;
    }

    private static int bufferOffset(final long packedValues)
    {
        return (int)(packedValues >>> 32);
    }

    private static int tokenIndex(final long packedValues)
    {
        return (int)packedValues;
    }
}
