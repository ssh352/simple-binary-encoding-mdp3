package uk.co.real_logic.sbe.read_cme_pcaps.readers;


import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.ScopeTracker;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.helpers.LineCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CleanTokenListener;
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
        int nextCaptureOffset = bufferOffset;


        final UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);

        Map<Integer, Integer> messageTypeMap = new HashMap<Integer, Integer>();

        LineCounter lineCounter = new LineCounter(prop.run_short);

        while (nextCaptureOffset < buffer.capacity()) {
            lineCounter.incrementLinesRead();

            final int headerLength = headerDecoder.encodedLength();

            PacketOffsets packetOffsets = new PacketOffsets(offsets, nextCaptureOffset, headerLength).invoke();
            int captureOffset = packetOffsets.getCaptureOffset();
            int packetOffset = packetOffsets.getPacketOffset();
            int headerStartOffset = packetOffsets.getHeaderStartOffset();
            int messageOffset = packetOffsets.getMessageOffset();


            int message_size = buffer.getShort(packetOffset + offsets.size_offset, offsets.message_size_endianness);
            long packet_sequence_number = buffer.getInt(packetOffset + offsets.packet_sequence_number_offset);
            long sendingTime = buffer.getLong(packetOffset + offsets.sending_time_offset);

            nextCaptureOffset = message_size + captureOffset + offsets.packet_size_padding;

            tablesHandler.setPacketValues(headerStartOffset, message_size, packet_sequence_number, sendingTime);

            final int templateId = headerDecoder.getTemplateId(buffer, headerStartOffset);
            final int actingVersion = headerDecoder.getSchemaVersion(buffer, headerStartOffset);
            final int blockLength = headerDecoder.getBlockLength(buffer, headerStartOffset);

            final List<Token> msgTokens = ir.getMessage(templateId);

            TokenListener tokenListener = new CleanTokenListener(tablesHandler, scopeTracker);
            OtfMessageDecoder.decode(
                    buffer,
                    messageOffset,
                    actingVersion,
                    blockLength,
                    msgTokens,
                    tokenListener);


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


    private static class PacketOffsets {
        private DataOffsets offsets;
        private int nextCaptureOffset;
        private int headerLength;
        private int captureOffset;
        private int packetOffset;
        private int headerStartOffset;
        private int messageOffset;

        public PacketOffsets(DataOffsets offsets, int nextCaptureOffset, int headerLength) {
            this.offsets = offsets;
            this.nextCaptureOffset = nextCaptureOffset;
            this.headerLength = headerLength;
        }

        public int getCaptureOffset() {
            return captureOffset;
        }

        public int getPacketOffset() {
            return packetOffset;
        }

        public int getHeaderStartOffset() {
            return headerStartOffset;
        }

        public int getMessageOffset() {
            return messageOffset;
        }

        public PacketOffsets invoke() {
            captureOffset = nextCaptureOffset;
            packetOffset = captureOffset;
            headerStartOffset = captureOffset + offsets.header_bytes;
            messageOffset = headerStartOffset + headerLength;
            return this;
        }
    }
}

