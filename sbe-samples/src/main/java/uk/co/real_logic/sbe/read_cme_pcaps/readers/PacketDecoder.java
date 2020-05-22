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
        this.buffer=buffer;
        this.tablesHandler=tablesHandler;
        this.nextPacketStartPosition=this.offsets.starting_offset; //skip leading bytes before message capture proper
    }


    public void setNewOffsets(int headerLength) throws IOException {
        this.packetStartPosition =nextPacketStartPosition;
        this.headerStartOffset = this.packetStartPosition + offsets.header_bytes;
        this.messageStartPosition= headerStartOffset + headerLength;
        this.decodePacketInfo();
        this.setNextPacketStartPosition();
    }
    private void decodePacketInfo() throws IOException {
        //todo: reduce duplication by making a method for getting the offset that includes packetCapture
        this.messageSize = this.buffer.getShort(this.packetStartPosition + this.offsets.size_offset, offsets.message_size_endianness);
        this.packetSequenceNumber = this.buffer.getInt(this.packetStartPosition + this.offsets.packet_sequence_number_offset);
        this.sendingTime = this.buffer.getLong(this.packetStartPosition + this.offsets.sending_time_offset);
        this.setPacketValues();
    }

    private void setNextPacketStartPosition(){
        this.nextPacketStartPosition=this.getNextPacketOffset();
    }

    public void setPacketValues() throws IOException {
        tablesHandler.setPacketValues(this.messageSize, this.packetSequenceNumber, this.sendingTime);
    }

    public int getNextPacketOffset() {
        this.nextPacketOffset = this.messageSize + this.packetStartPosition + this.offsets.packet_size_padding;
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


    public long  getSendingTime() {
        return sendingTime;
    }


}

