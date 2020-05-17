package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class SingleTableOutput {

    private Writer writer;
    private boolean first_row=true;
    private StringBuilder columnValues = new StringBuilder();
    private StringBuilder columnHeaders = new StringBuilder();

    public SingleTableOutput(String path, String tableName) throws IOException {
        this.writer=new FileWriter(path + tableName);
    }

    public void append(String columnName, String columnValue)  {
        if(this.first_row){
            this.appendColumn(this.columnHeaders, columnName);
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
        //setLength clears stringbuilder buffer
        this.columnValues.setLength(0);
        this.writer.flush();
    }


    public void close() throws IOException {
        this.writer.close();
    }
}
