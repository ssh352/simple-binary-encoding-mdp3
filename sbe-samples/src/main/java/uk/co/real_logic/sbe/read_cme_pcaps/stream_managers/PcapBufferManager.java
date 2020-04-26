package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

public class PcapBufferManager {
    private static DataOffsets offsets;
    final public UnsafeBuffer buffer;
    private int buffer_offset=0;
    private int header_offset;
    private int next_offset;
    public PcapBufferManager(DataOffsets offsets, UnsafeBuffer buffer) {
        this.buffer=buffer;
        this.offsets =offsets;
    }

    public UnsafeBuffer getBuffer(){
        return buffer;
    }



    public boolean nextOffsetValid(int nextOffset){
        return nextOffset < buffer.capacity();
    }





}
