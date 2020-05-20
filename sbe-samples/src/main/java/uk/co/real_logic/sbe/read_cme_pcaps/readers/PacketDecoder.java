package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

import java.io.IOException;

public class PacketDecoder {
    private final UnsafeBuffer buffer;
    private DataOffsets offsets;
    private int headerLength;
    private int packetCapturePosition;
    private int headerStartOffset;
    private int messageStartPosition;
    private TablesHandler tablesHandler;


    int messageSize;
    long packetSequenceNumber;
    long sendingTime;

    public PacketDecoder(DataOffsets offsets, UnsafeBuffer buffer, TablesHandler tablesHandler) {
        this.offsets = offsets;
        this.buffer=buffer;
        this.tablesHandler=tablesHandler;
    }

    public void setNewOffsets(int packetCapturePosition, int headerLength) throws IOException {
        this.packetCapturePosition=packetCapturePosition;
        this.headerLength = headerLength;
        this.headerStartOffset = this.packetCapturePosition + offsets.header_bytes;
        this.messageStartPosition= headerStartOffset + headerLength;
        this.decodePacketInfo();
    }
    private void decodePacketInfo() throws IOException {
        //todo: reduce duplication by making a method for getting the offset that includes packetCapture
        this.messageSize = this.buffer.getShort(this.packetCapturePosition + this.offsets.size_offset, offsets.message_size_endianness);
        this.packetSequenceNumber = this.buffer.getInt(this.packetCapturePosition + this.offsets.packet_sequence_number_offset);
        this.sendingTime = this.buffer.getLong(this.packetCapturePosition + this.offsets.sending_time_offset);
        this.setPacketValues();
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


    public void setPacketValues() throws IOException {
         tablesHandler.setPacketValues(this.messageSize, this.packetSequenceNumber, this.sendingTime);
    }

}

