package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

import java.nio.MappedByteBuffer;

public class PcapBufferManager {
    private static DataOffsets dataOffsets;
    final public UnsafeBuffer buffer;

    public PcapBufferManager(DataOffsets offsets, UnsafeBuffer buffer) {
        this.buffer=buffer;
    }

    public UnsafeBuffer getBuffer(){
        return buffer;
    }

    public boolean nextOffsetValid(int nextOffset){
        return nextOffset < buffer.capacity();
    }


}
