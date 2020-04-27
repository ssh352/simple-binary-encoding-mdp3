package uk.co.real_logic.sbe.read_cme_pcaps.readers;


import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.IrEncoder;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CompactTokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.TokenOutput;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.co.real_logic.sbe.tests.FileComparison.compare_files;


public class ReadPcaps {
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
//    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;
    private static final int SCHEMA_BUFFER_CAPACITY = 5000000 * 1024;

    public static void main(final String[] args) throws Exception {
        ReadPcapProperties prop = new ReadPcapProperties(args[0]);

        int message_index = 0;


        DataOffsets offsets = new DataOffsets(prop.data_source);
        Writer outWriter;
        // Encode up message and schema as if we just got them off the wire.


        if (prop.write_to_file) {
            outWriter = new FileWriter(prop.out_file);
            outWriter.flush();
        } else {
            outWriter = new PrintWriter(System.out, true);

        }

        TokenOutput tokenOutput = new TokenOutput(outWriter, true);

        final ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
        encodeSchema(encodedSchemaBuffer, prop.schema_file);
        RandomAccessFile aFile = new RandomAccessFile(prop.in_file, "rw");
        FileChannel inChannel = aFile.getChannel();
        long fileSize = inChannel.size();
        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
        encodedMsgBuffer.flip();  //make buffer ready for read
        // Now lets decode the schema IR so we have IR objects.
        encodedSchemaBuffer.flip();
        final Ir ir = decodeIr(encodedSchemaBuffer);
        final OtfHeaderDecoder headerDecoder = new OtfHeaderDecoder(ir.headerStructure());

        // Now we have IR we can read the message header

        int bufferOffset = offsets.starting_offset; //skip leading bytes before message capture proper
        int next_offset = bufferOffset;


        final UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);

        Map<Integer, Integer> messageTypeMap = new HashMap<Integer, Integer>();
        int blockLength;


        long num_lines = 500000000;
        int num_lines_short = 500000; //only run through part of buffer for debugging purposes
        if (prop.run_short) {
            num_lines = num_lines_short;
        }

        long sending_time = 0;
        long packet_sequence_number = 0;
        int lines_read = 0;


        while (next_offset < buffer.capacity()) {

            if (lines_read >= num_lines) {
                System.out.println("Read " + num_lines + " lines");
                break;
            }
            try {


                if ((lines_read * 1.0 / 10000 == lines_read / 10000)) {
                    System.out.println(lines_read);
                    System.out.println("sending_time: " + sending_time);
                }
                bufferOffset = next_offset;
                int message_size = buffer.getShort(bufferOffset + offsets.size_offset, offsets.message_size_endianness);
                packet_sequence_number = buffer.getInt(bufferOffset + offsets.packet_sequence_number_offset);
                sending_time = buffer.getLong(bufferOffset + offsets.sending_time_offset);
                next_offset = message_size + bufferOffset + offsets.packet_size_padding;
                bufferOffset = bufferOffset + offsets.header_bytes;

                final int templateId = headerDecoder.getTemplateId(buffer, bufferOffset);
                final int actingVersion = headerDecoder.getSchemaVersion(buffer, bufferOffset);
                blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);

                bufferOffset += headerDecoder.encodedLength();
                Integer count = messageTypeMap.getOrDefault(templateId, 0);
                messageTypeMap.put(templateId, count + 1);

                final List<Token> msgTokens = ir.getMessage(templateId);
                if (bufferOffset + blockLength >= fileSize) {
                    break;
                } else {
                    TokenListener tokenListener = new CompactTokenListener(tokenOutput, message_index, packet_sequence_number, sending_time, templateId, true);
                    OtfMessageDecoder.decode(
                            buffer,
                            bufferOffset,
                            actingVersion,
                            blockLength,
                            msgTokens,
                            tokenListener);
                }
                message_index++;
                outWriter.flush();
                lines_read = lines_read + 1;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("read next message failed");
                outWriter.flush();
            }


        }
        outWriter.close();
        inChannel.close();
        compare_files();
    }


    private static void encodeSchema(final ByteBuffer byteBuffer, String schema_file) throws Exception {
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


    private static Ir decodeIr(final ByteBuffer buffer) {
        try (IrDecoder irDecoder = new IrDecoder(buffer)) {
            return irDecoder.decode();
        }
    }


}

