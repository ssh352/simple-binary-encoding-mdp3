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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MarketDataExample {
    private static final MessageHeaderEncoder HEADER_ENCODER = new MessageHeaderEncoder();
    private static final CarEncoder CAR_ENCODER = new CarEncoder();
    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;
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
//        ByteBuffer encodedMsgBuffer = ByteBuffer.allocate(MSG_BUFFER_CAPACITY);
        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
//        System.out.println("inchannel size" + inChannel.size());
//        int bytesRead = inChannel.read(encodedMsgBuffer); //read into buffer.

        encodedMsgBuffer.flip();  //make buffer ready for read


        // Now lets decode the schema IR so we have IR objects.
        encodedSchemaBuffer.flip();
        final Ir ir = decodeIr(encodedSchemaBuffer);


        final OtfHeaderDecoder headerDecoder = new OtfHeaderDecoder(ir.headerStructure());


        // Now we have IR we can read the message header
        int bufferOffset = 0;


        final UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);


        // Given the header information we can select the appropriate message template to do the decode.
        // The OTF Java classes are thread safe so the same instances can be reused across multiple threads.
/*
        for(int i=0; i<msgTokens.size(); i++){
            System.out.println(msgTokens.get(i).toString());
        }
*/
        Map<Integer, Integer> messageTypeMap = new HashMap<Integer, Integer>();
        int blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);
        int bytes_to_skip=20;


        while (bufferOffset < 500000000) { //todo fix running to exact end of file/
        //print templates only
            int size_int = buffer.getShort(bufferOffset + 2);
            int next_offset =size_int + bufferOffset + 4;
            bufferOffset = bufferOffset + bytes_to_skip;
            int templateId=buffer.getShort(bufferOffset);
            System.out.println("offset: " + bufferOffset + " templateID: " + templateId + " nextOffset: " + next_offset);
            bufferOffset=next_offset;
        }
        while (bufferOffset < 500000000) { //todo fix running to exact end of file
//            System.out.println("buffer offset: " + bufferOffset);
            for(int i = 0;i < headerDecoder.encodedLength(); i++) {
//                System.out.println("byte " + i + ": " + buffer.getByte(i));
            }
            final int templateId = headerDecoder.getTemplateId(buffer, bufferOffset);
            final int schemaId = headerDecoder.getSchemaId(buffer, bufferOffset);
            final int actingVersion = headerDecoder.getSchemaVersion(buffer, bufferOffset);
            blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);

            bufferOffset += headerDecoder.encodedLength();
            // System.out.println("templateIdFromBuffer: " + templateId);
            // System.out.println("bufferOffset: " + bufferOffset);
            //System.out.println("blockLength: " + blockLength);
            Integer count = messageTypeMap.getOrDefault(templateId, 0);
            messageTypeMap.put(templateId, count + 1);
            if (ir.checkForMessage(templateId)) {

                final List<Token> msgTokens = ir.getMessage(templateId);
                if (bufferOffset + blockLength < inChannel.size()){
                bufferOffset = OtfMessageDecoder.decode(
                        buffer,
                        bufferOffset,
                        actingVersion,
                        blockLength,
                        msgTokens,
                        new ExampleTokenListener(new PrintWriter(System.out, true)));
                    bufferOffset = bufferOffset + blockLength+ bytes_to_skip;
                    blockLength = headerDecoder.getBlockLength(buffer, bufferOffset); //lookahead
//                    System.out.println("buffer offset: " + bufferOffset);
            }

            }
            //if (bufferOffset != encodedMsgBuffer.position()) {
            //   throw new IllegalStateException("Message not fully decoded");
            // }
        }
//        System.out.println("message frequency");
        for(int key : messageTypeMap.keySet()){
//            System.out.println (key + ": " + messageTypeMap.get(key));
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

