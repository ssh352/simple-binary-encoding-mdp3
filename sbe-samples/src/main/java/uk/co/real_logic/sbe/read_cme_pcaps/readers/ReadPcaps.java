package uk.co.real_logic.sbe.read_cme_pcaps.readers;


import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.PacketInfo.PacketInfo;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.ScopeTracker;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CleanTokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.TokenOutput;
import uk.co.real_logic.sbe.tests.DirectoryComparison;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReadPcaps {
    //    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;
    private static final int SCHEMA_BUFFER_CAPACITY = 5000000 * 1024;

    public static void main(final String[] args) throws Exception {
//        ReadPcapProperties prop = new ReadPcapProperties(args[0]);
        ReadPcapProperties prop = new ReadPcapProperties("C:\\marketdata\\testdata\\configs\\cleanlistener.config");


        DataOffsets offsets = new DataOffsets(prop.data_source);

        boolean compareToPreviousFiles = false;
        Writer residualOutWriter = new FileWriter("C:\\marketdata\\testdata\\separatetables\\residualoutput.txt");
        ScopeTracker scopeTracker = new ScopeTracker();
        TablesHandler tablesHandler = new TablesHandler("C:\\marketdata\\testdata\\separatetables\\latestresults\\", scopeTracker);


        RowCounter row_counter = new RowCounter();

        final ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
        EncoderDecoders.encodeSchema(encodedSchemaBuffer, prop.schema_file);
        RandomAccessFile aFile = new RandomAccessFile(prop.in_file, "rw");
        FileChannel inChannel = aFile.getChannel();
        long fileSize = inChannel.size();
        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
        encodedMsgBuffer.flip();  //make buffer ready for read
        // Now lets decode the schema IR so we have IR objects.
        encodedSchemaBuffer.flip();
        final Ir ir = EncoderDecoders.decodeIr(encodedSchemaBuffer);
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

        int lines_read = 0;

        while (next_offset < buffer.capacity()) {



            bufferOffset = next_offset;
            int message_size = buffer.getShort(bufferOffset + offsets.size_offset, offsets.message_size_endianness);
            long packet_sequence_number = buffer.getInt(bufferOffset + offsets.packet_sequence_number_offset);
            long sendingTime = buffer.getLong(bufferOffset + offsets.sending_time_offset);
            next_offset = message_size + bufferOffset + offsets.packet_size_padding;
            bufferOffset = bufferOffset + offsets.header_bytes;



            tablesHandler.setPacketValues(bufferOffset, message_size, packet_sequence_number, sendingTime);


            final int templateId = headerDecoder.getTemplateId(buffer, bufferOffset);
            PacketInfo packetInfo = new PacketInfo(templateId, packet_sequence_number, sendingTime);


            final int actingVersion = headerDecoder.getSchemaVersion(buffer, bufferOffset);
            blockLength = headerDecoder.getBlockLength(buffer, bufferOffset);

            bufferOffset += headerDecoder.encodedLength();
            Integer count = messageTypeMap.getOrDefault(templateId, 0);
            messageTypeMap.put(templateId, count + 1);

            final List<Token> msgTokens = ir.getMessage(templateId);
            if (bufferOffset + blockLength >= fileSize) {
                break;
            } else {
                TokenListener tokenListener = new CleanTokenListener(tablesHandler, scopeTracker);
                OtfMessageDecoder.decode(
                        buffer,
                        bufferOffset,
                        actingVersion,
                        blockLength,
                        msgTokens,
                        tokenListener);
            }


            if (lines_read >= num_lines) {
                System.out.println("Read " + num_lines + " lines");
                break;
            }
            displayProgress(lines_read, sendingTime);
            lines_read = lines_read + 1;


        }
        tablesHandler.close();
        inChannel.close();
        //test directory comparison by comparing same directory
        DirectoryComparison.compareDirectories("C:/marketdata/testdata/separatetables/referencedirectory/", "C:/marketdata/testdata/separatetables/latestresults/");
/*        if (compareToPreviousFiles) {
            String reference_file="c:/marketdata/testdata/separatetables/residualoutput_5_27.txt";
            String latest_output = "c:/marketdata/testdata/separatetables/residualoutput.txt";
            compare_files(reference_file, latest_output);
        }

 */
    }

    private static void displayProgress(int lines_read, long sendingTime) {
        if ((lines_read * 1.0 / 10000 == lines_read / 10000)) {
            System.out.println(lines_read);
            System.out.println("sending_time: " + sendingTime);
        }
    }


}

