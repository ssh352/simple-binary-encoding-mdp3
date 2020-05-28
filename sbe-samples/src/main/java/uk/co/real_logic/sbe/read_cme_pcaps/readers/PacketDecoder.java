package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

import java.io.IOException;

public class PacketDecoder {
    private final UnsafeBuffer buffer;
    private DataOffsets offsets;
    int packetStartPosition;
    private int headerStartOffset;
    private int messageStartPosition;
    private TablesHandler tablesHandler;
    public int nextPacketStartPosition;

    int messageSize;
    long packetSequenceNumber;
    long sendingTime;
    private int nextPacketOffset;

    public PacketDecoder(DataOffsets offsets, UnsafeBuffer buffer, TablesHandler tablesHandler) {
        this.offsets = offsets;
        this.buffer = buffer;
        this.tablesHandler = tablesHandler;
        this.nextPacketStartPosition = this.offsets.starting_offset; //skip leading bytes before message capture proper
    }


    public void processOffsets(int headerLength) throws IOException {
        this.packetStartPosition = nextPacketStartPosition;
        this.headerStartOffset = this.absoluteOffset(offsets.header_bytes);
        this.messageStartPosition = headerStartOffset + headerLength;
        this.decodePacketInfo();

        this.nextPacketStartPosition = this.getNextPacketOffset();

    }

    private void decodePacketInfo() throws IOException {
        //todo: reduce duplication by making a method for getting the offset that includes packetCapture
        messageSize = this.buffer.getShort(this.absoluteOffset(this.offsets.size_offset), offsets.message_size_endianness);
        packetSequenceNumber = this.buffer.getInt(this.absoluteOffset(this.offsets.packet_sequence_number_offset));
        sendingTime = this.buffer.getLong(this.absoluteOffset(this.offsets.sending_time_offset));
        this.setPacketValues(messageSize, packetSequenceNumber, sendingTime);
    }


    public void setPacketValues(int messageSize, long packetSequenceNumber, long sendingTime) throws IOException {
        tablesHandler.onBeginPacket(messageSize, packetSequenceNumber, sendingTime);
    }

    public int getNextPacketOffset() {
        this.nextPacketOffset = this.absoluteOffset(this.messageSize + this.offsets.packet_size_padding);
        return this.nextPacketOffset;
    }

    public int getMessageStartPosition() {
        return this.messageStartPosition;
    }

    public boolean hasNextPacket() {
        return getNextPacketOffset() < buffer.capacity();
    }

    public int getHeaderStartOffset() {
        return headerStartOffset;
    }


    public long getSendingTime() {
        return sendingTime;
    }

    public int absoluteOffset(int relativeOffset) {
        return this.packetStartPosition + relativeOffset;
    }


}

