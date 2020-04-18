package uk.co.real_logic.sbe.read_cme_pcaps.message_processing;

import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.MessageProcessorInputs;
import uk.co.real_logic.sbe.read_cme_pcaps.stream_managers.PcapBufferManager;
import uk.co.real_logic.sbe.read_cme_pcaps.tablebulders.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CompactTokenListener;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class MessageProcessor {


    public static void processMessages(MessageProcessorInputs message_processor_inputs, Writer outWriter) throws IOException {
        RowCounter row_counter = message_processor_inputs.row_counter;
        OtfHeaderDecoder header_decoder = message_processor_inputs.header_decoder;
        PcapBufferManager pcap_buffer_manager = message_processor_inputs.pcap_buffer_manager;
        Ir ir = message_processor_inputs.ir;

        int blockLength;
        while (pcap_buffer_manager.processNextOffset()) {
            try {

                setBufferProperties(row_counter, header_decoder, pcap_buffer_manager);

                int actingVersion = header_decoder.getSchemaVersion(pcap_buffer_manager.getBuffer(), pcap_buffer_manager.getHeaderOffset());
                blockLength = header_decoder.getBlockLength(pcap_buffer_manager.getBuffer(), pcap_buffer_manager.getHeaderOffset());
                pcap_buffer_manager.setTokenOffset(header_decoder.encodedLength());

                List<Token> msgTokens = ir.getMessage(row_counter.getTemplateId());
                final TokenListener tokenListener = new CompactTokenListener(outWriter, row_counter, true);
                decodeMessage(pcap_buffer_manager, blockLength, actingVersion, msgTokens, tokenListener);
                outWriter.flush();
                row_counter.increment_row_count();
            } catch (final Exception e) {
                e.printStackTrace();
                System.out.println("read next message failed");
                outWriter.flush();
            }

            //          System.out.print(" next buffer position " + String.valueOf(bufferManager.next_offset()) + "\n" );
        }
    }


    private static void setBufferProperties(RowCounter row_counter, OtfHeaderDecoder headerDecoder, PcapBufferManager bufferManager) {
        row_counter.setPacketSequenceNumber(bufferManager.packet_sequence_number());
        row_counter.setSending_time(bufferManager.sending_time());
        row_counter.setTemplateId(headerDecoder.getTemplateId(bufferManager.getBuffer(), bufferManager.getHeaderOffset()));
    }


    private static int decodeMessage(PcapBufferManager bufferManager, int blockLength, int actingVersion, List<Token> msgTokens, TokenListener tokenListener) throws IOException {
        return OtfMessageDecoder.decode(
                bufferManager.getBuffer(),
                bufferManager.getTokenOffset(),
                actingVersion,
                blockLength,
                msgTokens,
                tokenListener);
    }

}
