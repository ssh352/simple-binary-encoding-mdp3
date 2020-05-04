package uk.co.real_logic.sbe.read_cme_pcaps.token_listeners;

import uk.co.real_logic.sbe.read_cme_pcaps.PacketInfo.PacketInfo;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.CounterTypes;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;

import java.io.IOException;
import java.io.Writer;

public class TokenOutput {
    private final Writer out;
    boolean include_value_labels;
    RowCounter row_counter;
    PacketInfo packetInfo;

    public TokenOutput(Writer out, RowCounter row_counter, boolean include_value_labels) {
        this.row_counter = row_counter;
        this.out = out;
        this.include_value_labels = include_value_labels;
    }

    public void writeRowCounts(CompactTokenListener.RowType row_type) {

        writerOut(String.valueOf(this.row_counter.get_count(CounterTypes.MESSAGE_COUNT)));
        writeColumnValue(String.valueOf(this.row_counter.get_count((CounterTypes.GROUP_HEADER_COUNT))));
        writeColumnValue(String.valueOf(this.row_counter.get_count((CounterTypes.GROUP_ELEMENT_COUNT))));
        writeColumnValue(pad(row_type.toString(), 16, ' '));
    }

    public void setPacketInfo(PacketInfo packetInfo){
        this.packetInfo=packetInfo;
    }


    public void writePacketInfo() {
        String packet_sequence_number_string = String.format("%d", this.packetInfo.getPacketSequenceNumber());
        String templateID= String.format("%d", this.packetInfo.getTemplateID());
        //todo: get rid of event count.. it's redunant with packedid
        this.writeColumnValue(templateID);
        this.writeColumnValue(packet_sequence_number_string) ;
        this.writeColumnValue(String.format("%d", this.row_counter.get_count(CounterTypes.EVENT_COUNT)));
        this.writeColumnValue(packetInfo.getSendingTime());
        this.writeColumnValue(packetInfo.getTransactTime());
    }

    public String pad(String str, int size, char padChar) {
        StringBuffer padded = new StringBuffer(str);
        while (padded.length() < size) {
            padded.append(padChar);
        }
        return padded.toString();
    }

    void writeFieldValue(String field_label, String printableObject) {
        StringBuilder sb = new StringBuilder();
        if (this.include_value_labels) {
            sb.append(field_label);
            sb.append("=");
        }
        sb.append(printableObject);
        this.writeColumnValue(sb.toString());
        //here is where it prints the deep scope for each value.. we'd like to somehow
    }

    //todo: make row count adn packet info implement base class that has a .toString
    public void writeRowHeader(CompactTokenListener.RowType row_type, PacketInfo timestampTracker, String scopeString) {
        this.writeRowCounts(row_type);
        this.writePacketInfo();
        this.writerOut(scopeString);
    }

    void writeColumnValue(String columnValue){
        writerOut(", ");
        writerOut(columnValue);
    }

    void writerOut(String s) {
        try {
            this.out.write(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void flush() {
        try {
            this.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
