package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

public class PcapBufferManager {
    final public UnsafeBuffer buffer;
    private int buffer_offset=0;
    private int header_offset;
    private int next_offset;
    public PcapBufferManager(DataOffsets offsets, UnsafeBuffer buffer) {
        this.buffer=buffer;
    }

    public UnsafeBuffer getBuffer(){
        return this.buffer;
    }



    public boolean nextOffsetValid(int nextOffset){
        return nextOffset < buffer.capacity();
    }





}
