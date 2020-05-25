package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.counters.CounterTypes;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class SingleTableOutput {

    private static final int STRINGBUILDER_CAPACITY = 10000 * 1024;
    private final Writer writer;
    private boolean first_row=true;
    private boolean firstColumn=true;
    private final StringBuilder columnValues = new StringBuilder(STRINGBUILDER_CAPACITY);
    private final StringBuilder columnHeaders = new StringBuilder(STRINGBUILDER_CAPACITY);
    private final RowCounter rowCounter;

    public SingleTableOutput(String path, String tableName, RowCounter rowCounter) throws IOException {
        this.writer=new FileWriter(path + tableName);
        this.rowCounter = rowCounter;
    }

    public void append(String columnName, String columnValue)  {
        if(this.first_row){
            this.appendColumn(this.columnHeaders, columnName);
        }
        if(this.firstColumn){
            this.appendColumn(columnValues, String.valueOf(this.rowCounter.get_count(CounterTypes.EVENT_COUNT)));
        }
        this.appendColumn(this.columnValues, columnValue);
    }

    private void appendColumn(StringBuilder sb, String columnValue) {
        if (sb.length()>0) {
            sb.append(", ");
        }
        sb.append(columnValue);
    }


    public void completeRow() throws IOException {
        if(first_row){
            this.writer.append(columnHeaders.toString());
            this.writer.append("\n");
            this.first_row=false;
        }
        this.writer.append(columnValues.toString());
        this.writer.append("\n");
        this.columnValues.setLength(0);
        this.writer.flush();
    }

    public void close() throws IOException {
        this.writer.close();
    }
}
