package uk.co.real_logic.sbe.read_cme_pcaps;


import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.CMEPcapListener;
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
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;
    private static final int SCHEMA_BUFFER_CAPACITY = 1000 * 1024;

    public static void main(final String[] args) throws Exception {

        boolean run_short = false;
        boolean write_to_file;
        write_to_file=true;

        String data_source="ICE";
//        String data_source="CME";

        String binary_file_path;
        String out_file_path;
        //number of leading bytes for the whole file
        int starting_offset;
        //byte position in packet header of the packet size
        int size_offset;
        //byte position in packet header of the sending time size
        int sending_time_offset;
        //number of bytes in packet before template id
        int header_bytes;
        //number of bytes to adjust the packet size to jump from on header to the next
        int packet_size_padding;
        ByteOrder message_size_endianness;
        if(data_source=="ICE"){
            starting_offset=40; //what should this be?
            size_offset=16;
            message_size_endianness=ByteOrder.BIG_ENDIAN;
            sending_time_offset=46;
            write_to_file=true;
            header_bytes=56;
            packet_size_padding=30;
            binary_file_path = "c:/marketdata/ice_data/test_data/20191007.070000.080000.CME_GBX.CBOT.32_70.B.02.pcap.00014/20191007.070000.080000.CME_GBX.CBOT.32_70.B.02.pcap";

            out_file_path = "c:/marketdata/ice_parsed_compact_short";
        } else {

            starting_offset=0;
            size_offset=2;
            message_size_endianness=ByteOrder.LITTLE_ENDIAN;
            sending_time_offset=8;
            header_bytes=18;
            packet_size_padding=4;
            binary_file_path = "c:/marketdata/20191014-PCAP_316_0___0-20191014";
            out_file_path = "c:/marketdata/cme_parsed_compact_short_2";
        }

        long message_index = 0;
        Writer outWriter;
        message_index = 0;
        // Encode up message and schema as if we just got them off the wire.
        if(write_to_file){
            outWriter=new FileWriter(out_file_path);
//            outWriter.write("beginning of file");
            outWriter.flush();
        } else{
            outWriter=new PrintWriter(System.out, true);

        }

        final ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);

        String schema_file = "c:/marketdata/templates_FixBinary.xml";
        encodeSchema(encodedSchemaBuffer, schema_file);



        RandomAccessFile aFile = new RandomAccessFile(binary_file_path, "rw");
        FileChannel inChannel = aFile.getChannel();

        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());

        encodedMsgBuffer.flip();  //make buffer ready for read


        // Now lets decode the schema IR so we have IR objects.
        encodedSchemaBuffer.flip();
        final Ir ir = decodeIr(encodedSchemaBuffer);


        final OtfHeaderDecoder headerDecoder = new OtfHeaderDecoder(ir.headerStructure());


        // Now we have IR we can read the message header
        int bufferOffset = starting_offset; //skip leading bytes before message capture proper
        int next_offset = bufferOffset;



        final UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);

        Map<Integer, Integer> messageTypeMap = new HashMap<Integer, Integer>();
//        int blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);
        int blockLength;


        long num_lines = 500000000;
        int num_lines_short = 500000; //only run through part of buffer for debugging purposes
        if (run_short) {
            num_lines = num_lines_short;
        }


        System.out.println("first_capture byte: " + buffer.getByte(bufferOffset) );
        boolean keep_reading = true;
        while (keep_reading) { //todo fix running to exact end of file
            if(run_short & (bufferOffset > num_lines)){
                break;
            }
            try {
                bufferOffset = next_offset;
                int size_int = buffer.getShort(bufferOffset + size_offset, message_size_endianness);
//                int size_int_big = buffer.getShort(bufferOffset + size_offset, ByteOrder.BIG_ENDIAN);
//                int size_int_little = buffer.getShort(bufferOffset + size_offset, ByteOrder.LITTLE_ENDIAN);
                long sending_time = buffer.getLong(bufferOffset + sending_time_offset);
                next_offset = size_int + bufferOffset + packet_size_padding;
                bufferOffset = bufferOffset + header_bytes;

                final int templateId = headerDecoder.getTemplateId(buffer, bufferOffset);
                System.out.println("templateid:" + templateId);
                final int actingVersion = headerDecoder.getSchemaVersion(buffer, bufferOffset);
                blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);

                bufferOffset += headerDecoder.encodedLength();
                Integer count = messageTypeMap.getOrDefault(templateId, 0);
                messageTypeMap.put(templateId, count + 1);

                final List<Token> msgTokens = ir.getMessage(templateId);
                if (bufferOffset + blockLength < inChannel.size()) {
                    bufferOffset = OtfMessageDecoder.decode(
                            buffer,
                            bufferOffset,
                            actingVersion,
                            blockLength,
                            msgTokens,
                            new CompactTokenListener(outWriter, message_index, sending_time, templateId, false));
                }
                message_index++;
                outWriter.flush();
            } catch(Exception e) {
                outWriter.close();
                inChannel.close();
            }
    }
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

