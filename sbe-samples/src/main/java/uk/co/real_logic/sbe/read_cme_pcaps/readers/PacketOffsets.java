package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

import java.io.IOException;

public class PacketOffsets {
    private DataOffsets offsets;
    private int headerLength;
    private int packetCapturePosition;
    private int headerStartOffset;
    private int messageStartPosition;

    int messageSize;
    long packetSequenceNumber;
    long sendingTime;

    public PacketOffsets(DataOffsets offsets, UnsafeBuffer buffer, int packetCapturePosition,   int headerLength) {
        this.offsets = offsets;
        this.packetCapturePosition=packetCapturePosition;
        this.headerLength = headerLength;
        this.headerStartOffset = this.packetCapturePosition + offsets.header_bytes;
        this.messageStartPosition= headerStartOffset + headerLength;
        this.decodePacketInfo(buffer);
    }
    private void decodePacketInfo(UnsafeBuffer buffer) {
        this.messageSize = buffer.getShort(this.packetCapturePosition + this.offsets.size_offset, offsets.message_size_endianness);
        this.packetSequenceNumber = buffer.getInt(this.packetCapturePosition + this.offsets.packet_sequence_number_offset);
        this.sendingTime = buffer.getLong(this.packetCapturePosition + this.offsets.sending_time_offset);
    }

    public int getNextPacketOffset() {
        return this.messageSize + this.packetCapturePosition + this.offsets.packet_size_padding;

    }
        public int getMessageStartPosition() {
        return this.messageStartPosition;
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


    public void setPacketValues(TablesHandler tablesHandler) throws IOException {
         tablesHandler.setPacketValues(this.messageSize, this.packetSequenceNumber, this.sendingTime);
    }
}

