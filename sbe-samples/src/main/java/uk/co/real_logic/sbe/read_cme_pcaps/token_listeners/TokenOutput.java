package uk.co.real_logic.sbe.read_cme_pcaps.token_listeners;

import uk.co.real_logic.sbe.read_cme_pcaps.PacketInfo.PacketInfo;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.CounterTypes;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.TimestampTracker;

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
        writerOut(", ");
        writerOut(String.valueOf(this.row_counter.get_count((CounterTypes.GROUP_HEADER_COUNT))));
        writerOut(", ");
        writerOut(String.valueOf(this.row_counter.get_count((CounterTypes.GROUP_ELEMENT_COUNT))));
        writerOut(", ");
        writerOut(pad(row_type.toString(), 16, ' '));
    }

    public void setPacketInfo(PacketInfo packetInfo){
        this.packetInfo=packetInfo;
    }


    public void writePacketInfo(TimestampTracker timestampTracker) {
        String packet_sequence_number_string = String.format("%d", this.packetInfo.getPacketSequenceNumber());
        String templateID= String.format("%d", this.packetInfo.getTemplateID());
        String event_count_string = String.format("%d", this.row_counter.get_count(CounterTypes.EVENT_COUNT));
        this.writerOut(", " + templateID + ", " + packet_sequence_number_string + ", " + event_count_string + ", " + timestampTracker.getSending_time() + ", " + timestampTracker.getTransact_time());
    }

    public String pad(String str, int size, char padChar) {
        StringBuffer padded = new StringBuffer(str);
        while (padded.length() < size) {
            padded.append(padChar);
        }
        return padded.toString();
    }

    void writeFieldValue(String field_label, String printableObject) {
        this.writerOut(", ");
        if (this.include_value_labels) {
            this.writerOut(field_label);
            this.writerOut("=");
        }
        this.writerOut(printableObject);
        //here is where it prints the deep scope for each value.. we'd like to somehow
    }


    public void writeRowHeader(CompactTokenListener.RowType row_type, TimestampTracker timestampTracker, String scopeString) {
        this.writeRowCounts(row_type);
        this.writePacketInfo(packetInfo);
        this.writePacketInfo(timestampTracker);
        this.writerOut(scopeString);
    }

    private void writePacketInfo(PacketInfo packetInfo) {

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
