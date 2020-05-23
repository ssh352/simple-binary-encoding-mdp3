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
    private final Ir ir;
    private final OtfHeaderDecoder headerDecoder;
    private final UnsafeBuffer buffer;
    private TablesHandler tablesHandler;

    public PacketReader(ReadPcapProperties prop, BinaryDataHandler binaryDataHandler, TablesHandler tablesHandler)  {
        this.ir = binaryDataHandler.getIr();
        this.headerDecoder = binaryDataHandler.getHeaderDecoder();
        this.buffer = binaryDataHandler.getBuffer();
        this.offsets = new DataOffsets(prop.data_source);
        this.lineCounter = new LineCounter(prop.run_short);
        this.tablesHandler=tablesHandler;
    }

    //todo: figure out how to set packet values without passing in tablesHandler
    public void readPackets(TokenListener tokenListener) throws IOException {
        PacketDecoder packetDecoder = new PacketDecoder(offsets, buffer, this.tablesHandler);

        while (packetDecoder.hasNextPacket()) {
            packetDecoder.processOffsets(this.headerDecoder.encodedLength());
            decodeMessage(tokenListener, packetDecoder);

            this.lineCounter.incrementLinesRead(String.valueOf(packetDecoder.getSendingTime()));

        }
    }
    public void endPacketsCollection() throws IOException {
        this.tablesHandler.close();
    }


    private void decodeMessage(TokenListener tokenListener, PacketDecoder packetDecoder) throws IOException {

        final int templateId = headerDecoder.getTemplateId(buffer, packetDecoder.getHeaderStartOffset());
        final int actingVersion = headerDecoder.getSchemaVersion(buffer, packetDecoder.getHeaderStartOffset());
        final int blockLength = headerDecoder.getBlockLength(buffer, packetDecoder.getHeaderStartOffset());

        final int messageStartPosition = packetDecoder.getMessageStartPosition();

        final List<Token> msgTokens = ir.getMessage(templateId);

        //todo: wrap decoder in a different, and simpler, function
        OtfMessageDecoder.decode(
                buffer,
                messageStartPosition,
                actingVersion,
                blockLength,
                msgTokens,
                tokenListener);

    }


}
