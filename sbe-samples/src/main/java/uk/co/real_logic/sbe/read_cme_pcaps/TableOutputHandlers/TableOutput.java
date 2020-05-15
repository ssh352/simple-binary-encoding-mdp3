package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class TableOutput {

    private Writer writer;

    public TableOutput(Writer writer) {
        this.writer=writer;
    }

    public void outputAppend(String output) throws IOException {
       this.writer.append(output);
    }

}
