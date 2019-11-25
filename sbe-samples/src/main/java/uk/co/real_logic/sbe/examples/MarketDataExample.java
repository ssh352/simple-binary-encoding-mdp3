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

import baseline.CarEncoder;
import baseline.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.IrEncoder;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class MarketDataExample {
    private static final MessageHeaderEncoder HEADER_ENCODER = new MessageHeaderEncoder();
    private static final CarEncoder CAR_ENCODER = new CarEncoder();
    private static final int MSG_BUFFER_CAPACITY = 4 * 1024;
    private static final int SCHEMA_BUFFER_CAPACITY = 1000 * 1024;

    public static void main(final String[] args) throws Exception {
        System.out.println("\n*** OTF Example ***\n");

        // Encode up message and schema as if we just got them off the wire.
        final ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
        String schema_file = "c:/marketdata/templates_FixBinary.xml";
        encodeSchema(encodedSchemaBuffer, schema_file);


        String binary_file_path = "c:/marketdata/pcap_from_s3";


        RandomAccessFile aFile = new RandomAccessFile(binary_file_path, "rw");
        FileChannel inChannel = aFile.getChannel();

//create buffer with capacity of 48 bytes
        ByteBuffer encodedMsgBuffer = ByteBuffer.allocate(MSG_BUFFER_CAPACITY);

        int bytesRead = inChannel.read(encodedMsgBuffer); //read into buffer.

            encodedMsgBuffer.flip();  //make buffer ready for read


            // Now lets decode the schema IR so we have IR objects.
            encodedSchemaBuffer.flip();
            final Ir ir = decodeIr(encodedSchemaBuffer);


            final OtfHeaderDecoder headerDecoder = new OtfHeaderDecoder(ir.headerStructure());

            // Now we have IR we can read the message header
            int bufferOffset = 0;


            final UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);

            final int templateId = headerDecoder.getTemplateId(buffer, bufferOffset);
            final int schemaId = headerDecoder.getSchemaId(buffer, bufferOffset);
            final int actingVersion = headerDecoder.getSchemaVersion(buffer, bufferOffset);
            final int blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);

            bufferOffset += headerDecoder.encodedLength();

            // Given the header information we can select the appropriate message template to do the decode.
            // The OTF Java classes are thread safe so the same instances can be reused across multiple threads.
/*
        for(int i=0; i<msgTokens.size(); i++){
            System.out.println(msgTokens.get(i).toString());
        }
*/
        final List<Token> msgTokens = ir.getMessage(templateId);


            bufferOffset = OtfMessageDecoder.decode(
                    buffer,
                    bufferOffset,
                    actingVersion,
                    blockLength,
                    msgTokens,
                    new ExampleTokenListener(new PrintWriter(System.out, true)));

            if (bufferOffset != encodedMsgBuffer.position()) {
                throw new IllegalStateException("Message not fully decoded");
            }
        }

        private static void encodeSchema ( final ByteBuffer byteBuffer, String schema_file) throws Exception
        {
            File initialFile = new File(schema_file);
            InputStream targetStream = new FileInputStream(initialFile);
            try (InputStream in = new BufferedInputStream(targetStream)) {
                final MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
                final Ir ir = new IrGenerator().generate(schema);
                try (IrEncoder irEncoder = new IrEncoder(byteBuffer, ir)) {
                    irEncoder.encode();
                }
            }
        }



    private static void encodeTestMessage ( final ByteBuffer byteBuffer, String binary_file)
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

            final int encodedLength = ExampleUsingGeneratedStub.encode(CAR_ENCODER, buffer, HEADER_ENCODER);

            byteBuffer.position(encodedLength);
        }

        private static Ir decodeIr ( final ByteBuffer buffer)
    {
        try (IrDecoder irDecoder = new IrDecoder(buffer)) {
            return irDecoder.decode();
        }
    }




}

