package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

public class PcapBufferManager {
    private static DataOffsets offsets;
    final public UnsafeBuffer buffer;
    private int buffer_offset = 0;
    private int header_offset;
    private int next_offset;
    private int token_offset;

    public PcapBufferManager(DataOffsets offsets, UnsafeBuffer buffer) {
        this.buffer = buffer;
        PcapBufferManager.offsets = offsets;
    }

    public UnsafeBuffer getBuffer() {
        return buffer;
    }

    public void incrementPacket() {
        this.setBufferOffset(this.next_offset);
    }

    public int getTokenOffset() {
        return this.token_offset;
    }

    public void setTokenOffset(int header_length) {
        this.token_offset = this.buffer_offset + header_length;
    }

    public int getBufferOffset() {
        return this.buffer_offset;
    }

    public void setBufferOffset(int buffer_offset) {
        this.buffer_offset = buffer_offset;
        this.header_offset = this.buffer_offset + offsets.header_bytes;
        this.next_offset = calculate_next_offset();
    }

    public int getHeaderOffset() {
        return this.header_offset;
    }

    public boolean nextOffsetValid() {
        return this.next_offset < buffer.capacity();
    }

    public int message_size() {
        short message_size = buffer.getShort(buffer_offset + offsets.size_offset, offsets.message_size_endianness);
        return message_size;
    }


    public int packet_sequence_number() {
        return buffer.getInt(buffer_offset + offsets.packet_sequence_number_offset);
    }

    public long sending_time() {
        return buffer.getLong(buffer_offset + offsets.sending_time_offset);
    }

    private int calculate_next_offset() {
        return buffer_offset + message_size() + offsets.packet_size_padding;
    }

    public int next_offset() {
        return this.next_offset;
    }


}
