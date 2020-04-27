package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;

public class PcapBufferManager {
    final public UnsafeBuffer buffer;
    public PcapBufferManager(UnsafeBuffer buffer) {
        this.buffer = buffer;
    }

    public UnsafeBuffer getBuffer(){
        return this.buffer;
    }



    public boolean nextOffsetValid(int nextOffset){
        return nextOffset < this.buffer.capacity();
    }





}
