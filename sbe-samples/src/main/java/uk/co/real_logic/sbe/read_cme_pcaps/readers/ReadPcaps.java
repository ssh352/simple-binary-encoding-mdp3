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
import uk.co.real_logic.sbe.read_cme_pcaps.stream_managers.PcapBufferManager;
import uk.co.real_logic.sbe.read_cme_pcaps.tablebulders.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CompactTokenListener;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;


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

    public static void main(String[] args) throws Exception {
        ReadPcapProperties prop = new ReadPcapProperties(args[0]);

        RowCounter row_counter = new RowCounter();


        DataOffsets offsets = new DataOffsets(prop.data_source);
        Writer outWriter;


        outWriter = getWriter(prop);

        ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
        encodeSchema(encodedSchemaBuffer, prop.schema_file);
        RandomAccessFile aFile = new RandomAccessFile(prop.in_file, "rw");
        final FileChannel inChannel = aFile.getChannel();
        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
        encodedMsgBuffer.flip();  //make buffer ready for read
        // Now lets decode the schema IR so we have IR objects.
        encodedSchemaBuffer.flip();
        Ir ir = decodeIr(encodedSchemaBuffer);
        OtfHeaderDecoder headerDecoder = new OtfHeaderDecoder(ir.headerStructure());

        // Now we have IR we can read the message header


        UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);

        final long max_buffers_to_process= getNumLines(prop);
        PcapBufferManager bufferManager = new PcapBufferManager(offsets, buffer, max_buffers_to_process);

        bufferManager.setBufferOffset(offsets.starting_offset); //skip leading bytes before message capture proper

        int blockLength;

        while (bufferManager.processNextOffset()) {
            try {

                row_counter.setPacketSequenceNumber(bufferManager.packet_sequence_number());
                row_counter.setSending_time(bufferManager.sending_time());


                row_counter.setTemplateId(headerDecoder.getTemplateId(bufferManager.getBuffer(), bufferManager.getHeaderOffset()));
                int actingVersion = headerDecoder.getSchemaVersion(bufferManager.getBuffer(), bufferManager.getHeaderOffset());
                blockLength = headerDecoder.getBlockLength(bufferManager.getBuffer(), bufferManager.getHeaderOffset());

                bufferManager.setTokenOffset(headerDecoder.encodedLength());

                List<Token> msgTokens = ir.getMessage(row_counter.getTemplateId());
                final TokenListener tokenListener = new CompactTokenListener(outWriter,row_counter, true);
//                TokenListener tokenListener= new CMEPcapListener(outWriter, true, templateId);
                OtfMessageDecoder.decode(
                            bufferManager.getBuffer(),
                            bufferManager.getTokenOffset(),
                            actingVersion,
                            blockLength,
                            msgTokens,
                            tokenListener);
                outWriter.flush();
                row_counter.increment_row_count();
            } catch (final Exception e) {
                e.printStackTrace();
                System.out.println("read next message failed");
                outWriter.flush();
            }

  //          System.out.print(" next buffer position " + String.valueOf(bufferManager.next_offset()) + "\n" );
        }
        outWriter.close();
        inChannel.close();
    }

    private static Writer getWriter(final ReadPcapProperties prop) throws IOException {
        final Writer outWriter;
        if (prop.write_to_file) {
            outWriter = new FileWriter(prop.out_file);
//            outWriter.write("beginning of file");
            outWriter.flush();
        } else {
            outWriter = new PrintWriter(System.out, true);

        }
        return outWriter;
    }

    private static long getNumLines(final ReadPcapProperties prop) {
        long num_lines = 500000000;
        final int num_lines_short = 5000; //only run through part of buffer for debugging purposes
        if (prop.run_short) {
            num_lines = num_lines_short;
        }
        return num_lines;
    }


    private static void encodeSchema(ByteBuffer byteBuffer, final String schema_file) throws Exception {
        final File initialFile = new File(schema_file);
        final InputStream targetStream = new FileInputStream(initialFile);
        try (final InputStream in = new BufferedInputStream(targetStream)) {
            MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
            Ir ir = new IrGenerator().generate(schema);
            try (final IrEncoder irEncoder = new IrEncoder(byteBuffer, ir)) {
                irEncoder.encode();
            }
        }
    }


    private static Ir decodeIr(ByteBuffer buffer) {
        try (final IrDecoder irDecoder = new IrDecoder(buffer)) {
            return irDecoder.decode();
        }
    }


}

