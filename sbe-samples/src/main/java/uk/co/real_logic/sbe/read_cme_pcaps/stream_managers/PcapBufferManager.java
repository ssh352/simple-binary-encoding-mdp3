package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

import java.nio.MappedByteBuffer;

public class PcapBufferManager {
    private static DataOffsets dataOffsets;
    final public UnsafeBuffer buffer;
    private int buffer_offset=0;
    public PcapBufferManager(DataOffsets offsets, UnsafeBuffer buffer) {
        this.buffer=buffer;
    }

    public UnsafeBuffer getBuffer(){
        return buffer;
    }

    public void setBufferOffset(int buffer_offset){
        this.buffer_offset=buffer_offset;
    }

    public void advanceBufferOffset(int increment){
        this.buffer_offset+=increment;
    }
    public int getBufferOffset() {
        return this.buffer_offset;
    }

    public boolean nextOffsetValid(int nextOffset){
        return nextOffset < buffer.capacity();
    }

//    public boolean message_size(


}
