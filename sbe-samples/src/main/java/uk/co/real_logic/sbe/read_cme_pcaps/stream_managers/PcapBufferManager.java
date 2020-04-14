package uk.co.real_logic.sbe.read_cme_pcaps.stream_managers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

public class PcapBufferManager {
    private static DataOffsets dataOffsets;
    private UnsafeBuffer buffer;

    public PcapBufferManager(DataOffsets offsets, encodedMsgBuffer) {
        this.buffer=buffer;
    }

    public UnsafeBuffer getBuffer(){
        return buffer;
    }

    public boolean nextOffsetValid(int nextOffset){
        return nextOffset<buffer.capacity();
    }


}
