package uk.co.real_logic.sbe.read_cme_pcaps.properties;

import java.nio.ByteOrder;

public class DataOffsets {
    //number of leading bytes for the whole file
    public final int starting_offset;
    //byte position in packet header of the packet size
    public final int size_offset;
    //byte position in packet header of the sending time size
    public final int packet_sequence_number_offset;
    public final int sending_time_offset;
    //number of bytes in packet before template id
    public final int header_bytes;
    //number of bytes to adjust the packet size to jump from on header to the next
    public final int packet_size_padding;
    public final String data_source;
    public final ByteOrder message_size_endianness;

        public DataOffsets(String data_source){//todo: change data source to enum
            this.data_source = data_source;
            if(data_source.equals("ICE")){
                this.starting_offset=40; //what should this be?
                size_offset=16;
                message_size_endianness=ByteOrder.BIG_ENDIAN;
                packet_sequence_number_offset=42;
                sending_time_offset=46;
                header_bytes=56;
                packet_size_padding=30;
            } else {

                starting_offset=0;
                size_offset=2;
                message_size_endianness=ByteOrder.LITTLE_ENDIAN;
                packet_sequence_number_offset=4;
                sending_time_offset=8;
                header_bytes=18;
                packet_size_padding=4;
            }
        }

}
