package uk.co.real_logic.sbe.read_cme_pcaps;

import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.read_cme_pcaps.stream_managers.PcapBufferManager;
import uk.co.real_logic.sbe.read_cme_pcaps.tablebulders.RowCounter;


public class MessageProcessorInputs {
    public final RowCounter row_counter;
    public final OtfHeaderDecoder header_decoder;
    public final Ir ir;
    public final PcapBufferManager pcap_buffer_manager;

    public MessageProcessorInputs(RowCounter row_counter, OtfHeaderDecoder header_decoder, Ir ir, PcapBufferManager pcap_buffer_manager) {
        this.row_counter = row_counter;
        this.ir = ir;
        this.header_decoder = header_decoder;
        this.pcap_buffer_manager = pcap_buffer_manager;
    }
}
