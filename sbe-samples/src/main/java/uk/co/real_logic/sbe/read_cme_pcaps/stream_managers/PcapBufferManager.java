package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;

public class PcapBufferManager {
    private static DataOffsets offsets;
    final public UnsafeBuffer buffer;
    private int buffer_offset = 0;
    private int header_offset;
    private int next_offset;
    private int token_offset;
    final private long max_buffers_to_process;
    private long buffers_processed =0;
    private ReadPcapProperties prop;

    public PcapBufferManager(ReadPcapProperties prop, DataOffsets offsets, UnsafeBuffer buffer) {
        this.prop = prop;
        this.max_buffers_to_process = getNumLines();
        this.buffer = buffer;
        PcapBufferManager.offsets = offsets;
    }

    public UnsafeBuffer getBuffer() {
        return buffer;
    }

    public void incrementPacket() {
        this.setBufferOffset(this.next_offset);
        this.buffers_processed++;
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

    public boolean processNextOffset() {
        Boolean is_valid=true;
        if (this.next_offset >= buffer.capacity()){
            is_valid = false;
        }
        if (buffers_processed >max_buffers_to_process){
            is_valid=false;
        }
        buffers_processed++;
        return is_valid;
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


    private long getNumLines() {
        long num_lines = 500000000;
        final int num_lines_short = 5000; //only run through part of buffer for debugging purposes
        if (this.prop.run_short) {
            num_lines = num_lines_short;
        }
        return num_lines;
    }



}
