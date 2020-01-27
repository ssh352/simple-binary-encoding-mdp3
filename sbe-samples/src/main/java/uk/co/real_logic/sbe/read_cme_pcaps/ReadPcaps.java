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
//    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;
    private static final int SCHEMA_BUFFER_CAPACITY = 5000000 * 1024;

    public static void main(final String[] args) throws Exception {
        String os_string= System.getProperty("os.name").toLowerCase();
        String in_file = args[0];
        String out_file = args[1];
        System.out.println("in_file=" + in_file);
        System.out.println("out_file=" + out_file);
        int message_index=0;

        String schema_file;
        if(os_string.equals("linux")){
           schema_file = "/marketdata/templates_FixBinary.xml"; 
        } else{
           schema_file = "c:/marketdata/templates_FixBinary.xml";
        }
        boolean run_short = false;
        boolean write_to_file;
        write_to_file=true;

        String data_source="ICE";
//        String data_source="CME";

        String binary_file_path = in_file;
        String out_file_path= out_file;
        //number of leading bytes for the whole file
        int starting_offset;
        //byte position in packet header of the packet size
        int size_offset;
        //byte position in packet header of the sending time size
        int packet_sequence_number_offset;
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
            packet_sequence_number_offset=42;
            sending_time_offset=46;
            header_bytes=56;
            packet_size_padding=30;
        } else {

            starting_offset=0;
            size_offset=2;
            message_size_endianness=ByteOrder.LITTLE_ENDIAN;
            packet_sequence_number_offset=4;
            sending_time_offset=8;
            header_bytes=18;
            packet_size_padding=4;
//            binary_file_path = "c:/marketdata/20191014-PCAP_316_0___0-20191014";
//            out_file_path = "c:/marketdata/cme_parsed_compact_short_2";
        }
        Writer outWriter;
        // Encode up message and schema as if we just got them off the wire.
        if(write_to_file){
            outWriter=new FileWriter(out_file_path);
//            outWriter.write("beginning of file");
            outWriter.flush();
        } else{
            outWriter=new PrintWriter(System.out, true);

        }

        final ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);

        encodeSchema(encodedSchemaBuffer, schema_file);



        RandomAccessFile aFile = new RandomAccessFile(binary_file_path, "rw");
        FileChannel inChannel = aFile.getChannel();
        long fileSize=inChannel.size();
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

        long sending_time=0;
        long packet_sequence_number=0;
        int lines_read=0;
        System.out.println("first_capture byte: " + buffer.getByte(bufferOffset) );
        while (next_offset < buffer.capacity()) {
            if(lines_read >= num_lines ){
                System.out.println("Read " + num_lines +" lines");
                break;
            }
            try {
                if((lines_read*1.0/10000==lines_read/10000) ){
                    System.out.println(lines_read);
                    System.out.println("sending_time: " + sending_time);
                }
                bufferOffset = next_offset;
                int size_int = buffer.getShort(bufferOffset + size_offset, message_size_endianness);
                packet_sequence_number= buffer.getInt(bufferOffset + packet_sequence_number_offset);
                sending_time = buffer.getLong(bufferOffset + sending_time_offset);
                next_offset = size_int + bufferOffset + packet_size_padding;
                bufferOffset = bufferOffset + header_bytes;

                final int templateId = headerDecoder.getTemplateId(buffer, bufferOffset);
                final int actingVersion = headerDecoder.getSchemaVersion(buffer, bufferOffset);
                blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);

                bufferOffset += headerDecoder.encodedLength();
                Integer count = messageTypeMap.getOrDefault(templateId, 0);
                messageTypeMap.put(templateId, count + 1);

                final List<Token> msgTokens = ir.getMessage(templateId);
                if (bufferOffset + blockLength < fileSize) {
                    bufferOffset = OtfMessageDecoder.decode(
                            buffer,
                            bufferOffset,
                            actingVersion,
                            blockLength,
                            msgTokens,
                            new CompactTokenListener(outWriter, message_index,  packet_sequence_number, sending_time, templateId, false));
                } else{
                    break;
                }
                message_index++;
                outWriter.flush();
                lines_read = lines_read + 1;
            } catch(Exception e) {
                e.printStackTrace();
                System.out.println("read next message failed");
                outWriter.flush();
            }


        }
        outWriter.close();
        inChannel.close();
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

