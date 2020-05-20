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
        int nextPacketCapturePosition = bufferOffset;
        int packetCapturePosition = bufferOffset;
        while (nextPacketCapturePosition < this.buffer.capacity()) {
            final int headerLength = this.headerDecoder.encodedLength();

            packetCapturePosition = nextPacketCapturePosition;

            PacketOffsets packetOffsets = new PacketOffsets(offsets,buffer, packetCapturePosition,   headerLength);
            nextPacketCapturePosition = packetOffsets.getNextPacketOffset();
            packetOffsets.setPacketValues(tablesHandler);
            final int messageStartPosition=packetOffsets.getMessageStartPosition();

            final int templateId = headerDecoder.getTemplateId(buffer, packetOffsets.getHeaderStartOffset());
            final int actingVersion = headerDecoder.getSchemaVersion(buffer, packetOffsets.getHeaderStartOffset());
            final int blockLength = headerDecoder.getBlockLength(buffer, packetOffsets.getHeaderStartOffset());

            final List<Token> msgTokens = ir.getMessage(templateId);

            OtfMessageDecoder.decode(
                    buffer,
                    messageStartPosition,
                    actingVersion,
                    blockLength,
                    msgTokens,
                    tokenListener);


            this.lineCounter.incrementLinesRead(String.valueOf(packetOffsets.getSendingTime()));
        }
        tablesHandler.close();
    }


}
