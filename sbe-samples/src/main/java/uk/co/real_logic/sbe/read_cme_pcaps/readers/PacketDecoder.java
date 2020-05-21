package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

import java.io.IOException;

public class PacketDecoder {
    private final UnsafeBuffer buffer;
    private DataOffsets offsets;
    private int headerLength;
    int packetStartPosition;
    private int headerStartOffset;
    private int messageStartPosition;
    private TablesHandler tablesHandler;
    public int nextPacketStartPosition;

    int messageSize;
    long packetSequenceNumber;
    long sendingTime;

    public PacketDecoder(DataOffsets offsets, UnsafeBuffer buffer, TablesHandler tablesHandler) {
        this.offsets = offsets;
        this.buffer=buffer;
        this.tablesHandler=tablesHandler;
    }

    public void setNewOffsets(int packetCapturePosition, int headerLength) throws IOException {
        this.packetStartPosition =packetCapturePosition;
        this.headerLength = headerLength;
        this.headerStartOffset = this.packetStartPosition + offsets.header_bytes;
        this.messageStartPosition= headerStartOffset + headerLength;
        this.decodePacketInfo();
    }
    private void decodePacketInfo() throws IOException {
        //todo: reduce duplication by making a method for getting the offset that includes packetCapture
        this.messageSize = this.buffer.getShort(this.packetStartPosition + this.offsets.size_offset, offsets.message_size_endianness);
        this.packetSequenceNumber = this.buffer.getInt(this.packetStartPosition + this.offsets.packet_sequence_number_offset);
        this.sendingTime = this.buffer.getLong(this.packetStartPosition + this.offsets.sending_time_offset);
        this.setPacketValues();
    }

    public void setNextPacketStartPosition(int newPosition){
        this.nextPacketStartPosition=newPosition;
    }

    public int getNextPacketOffset() {
        return this.messageSize + this.packetStartPosition + this.offsets.packet_size_padding;

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


    public int getMessageSize() {
        return messageSize;
    }

    public long getPacketSequenceNumber() {
        return packetSequenceNumber;
    }

    public long  getSendingTime() {
        return sendingTime;
    }


    public void setPacketValues() throws IOException {
         tablesHandler.setPacketValues(this.messageSize, this.packetSequenceNumber, this.sendingTime);
    }

}

