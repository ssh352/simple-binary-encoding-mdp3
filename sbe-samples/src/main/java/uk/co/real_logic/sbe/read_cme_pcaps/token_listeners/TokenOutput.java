package uk.co.real_logic.sbe.read_cme_pcaps.token_listeners;

import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;

import java.io.IOException;
import java.io.Writer;

public class TokenOutput {
    private final Writer out;
    boolean include_value_labels;
    RowCounter row_counter;

    public TokenOutput(Writer out, RowCounter row_counter, boolean include_value_labels) {
        this.row_counter = row_counter;
        this.out = out;
        this.include_value_labels = include_value_labels;
    }

    public void writeRow(CompactTokenListener.RowType row_type, long message_count, long group_header_count, long group_element_count) {

        writerOut(String.valueOf(message_count));
        writerOut(", ");
        writerOut(String.valueOf(group_header_count));
        writerOut(", ");
        writerOut(String.valueOf(group_element_count));
        writerOut(", ");
        writerOut(pad(row_type.toString(), 16, ' '));
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
