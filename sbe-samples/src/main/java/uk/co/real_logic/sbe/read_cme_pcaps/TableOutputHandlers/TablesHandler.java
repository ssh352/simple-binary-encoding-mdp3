package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import java.io.IOException;
import java.util.HashMap;

public class TablesHandler {
    HashMap<String, SingleTableOutput> singleTablesOutput= new HashMap<>();
    String path;
    public TablesHandler(String path){
        this.path=path;
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


}
