package uk.co.real_logic.sbe.read_cme_pcaps.token_listeners;

import uk.co.real_logic.sbe.read_cme_pcaps.counters.CounterTypes;
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

    public void writeRow(CompactTokenListener.RowType row_type) {

        writerOut(String.valueOf(this.row_counter.get_count(CounterTypes.MESSAGE_COUNT)));
        writerOut(", ");
        writerOut(String.valueOf(this.row_counter.get_count((CounterTypes.GROUP_HEADER_COUNT))));
        writerOut(", ");
        writerOut(String.valueOf(this.row_counter.get_count((CounterTypes.GROUP_ELEMENT_COUNT))));
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
