package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

public class TablesHandler {
    HashMap<String, SingleTableOutput> singleTablesOutput= new HashMap<>();
    String path;
    Writer residualOutput;
    //todo: get rid of reference to explicit outwriter in token listener.. override append
    //eventually get rid of residualOutput
    public TablesHandler(String path, Writer residualOutput){
        this.path=path;
        this.residualOutput=residualOutput;
    }

    public void addTable( String tableName) throws IOException {
        SingleTableOutput newTableOutput= new SingleTableOutput(this.path, tableName);
        singleTablesOutput.put(tableName, newTableOutput) ;
//        singleTablesOutput.put(tableName, new SingleTableOutput(this.path, tableName)) ;

    }

    public void append(String tableName, String columnName, String value){
        singleTablesOutput.get(tableName).append(columnName, value);
    }

    public  void completeRow(String tableName) throws IOException {
        singleTablesOutput.get(tableName).completeRow();
    };

    public void flush() throws IOException {
        this.residualOutput.flush();
    }


    public TablesHandler append(String value) throws IOException {
        this.residualOutput.append(value);
        return this;
    }

    public void close() throws IOException {
        this.residualOutput.close();
    }
}
