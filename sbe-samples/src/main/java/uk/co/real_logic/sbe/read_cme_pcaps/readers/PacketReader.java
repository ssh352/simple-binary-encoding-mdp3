package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.helpers.LineCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CleanTokenListener;

import java.io.IOException;
import java.util.List;

public class PacketReader {

    private final DataOffsets offsets;
    private final LineCounter lineCounter;
    private final BinaryDataHandler binaryDataHandler;
    private final Ir ir;
    private final OtfHeaderDecoder headerDecoder;
    private final UnsafeBuffer buffer;


    public PacketReader(ReadPcapProperties prop, BinaryDataHandler binaryDataHandler) throws IOException {
        this.binaryDataHandler = binaryDataHandler;
        this.ir = binaryDataHandler.getIr();
        this.headerDecoder = binaryDataHandler.getHeaderDecoder();
        this.buffer = binaryDataHandler.getBuffer();
        this.offsets = new DataOffsets(prop.data_source);
        this.lineCounter = new LineCounter(prop.run_short);
    }

    //todo: figure out how to set packet values without passing in tablesHandler
    protected void readPackets(TokenListener tokenListener, TablesHandler tablesHandler) throws IOException {
        int bufferOffset = this.offsets.starting_offset; //skip leading bytes before message capture proper
        int nextCaptureOffset = bufferOffset;

        while (nextCaptureOffset < this.buffer.capacity()) {
            this.lineCounter.incrementLinesRead();
            final int headerLength = this.headerDecoder.encodedLength();
            PacketOffsets packetOffsets = new PacketOffsets(offsets, nextCaptureOffset, headerLength).invoke();

            int message_size = buffer.getShort(packetOffsets.getPacketOffset() + this.offsets.size_offset, offsets.message_size_endianness);
            long packet_sequence_number = buffer.getInt(packetOffsets.getPacketOffset() + this.offsets.packet_sequence_number_offset);
            long sendingTime = buffer.getLong(packetOffsets.getPacketOffset()+ this.offsets.sending_time_offset);

            nextCaptureOffset = message_size + packetOffsets.getCaptureOffset() + this.offsets.packet_size_padding;
            tablesHandler.setPacketValues(packetOffsets.getHeaderStartOffset(), message_size, packet_sequence_number, sendingTime);

            final int templateId = headerDecoder.getTemplateId(buffer, packetOffsets.getHeaderStartOffset());
            final int actingVersion = headerDecoder.getSchemaVersion(buffer, packetOffsets.getHeaderStartOffset());
            final int blockLength = headerDecoder.getBlockLength(buffer, packetOffsets.getHeaderStartOffset());

            final List<Token> msgTokens = ir.getMessage(templateId);

            OtfMessageDecoder.decode(
                    buffer,
                    packetOffsets.getMessageOffset(),
                    actingVersion,
                    blockLength,
                    msgTokens,
                    tokenListener);


        }
        tablesHandler.close();
    }


}
