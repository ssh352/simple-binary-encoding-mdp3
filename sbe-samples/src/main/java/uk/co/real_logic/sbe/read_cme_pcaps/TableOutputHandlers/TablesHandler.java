package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.io.IOException;
import java.util.HashMap;

import static uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel.*;

public class TablesHandler {
    final HashMap<String, SingleTableOutput> singleTablesOutput= new HashMap<>();
    String currentTable;
    final String path;
    private final ScopeTracker scopeTracker;
    public TablesHandler(String path, ScopeTracker scopeTracker){
        this.path=path;
        this.scopeTracker=scopeTracker;
    }

    public void addTable( String tableName) throws IOException {
        if(!singleTablesOutput.containsKey(tableName)){
        SingleTableOutput newTableOutput= new SingleTableOutput(this.path, tableName);
        singleTablesOutput.put(tableName, newTableOutput) ;}
    }

    private void appendToTable(String columnName, String value){
        singleTablesOutput.get(this.currentTable).append(columnName, value);
    }

    public  void completeRow(String tableName) throws IOException {
        singleTablesOutput.get(tableName).completeRow();
    }

    public void appendColumnValue(String columnName, String value){
        switch(scopeTracker.getScopeLevel()){
            case PACKET_HEADER:
                this.currentTable="packetheaders";
                this.appendToTable( columnName, value);
                break;
            case MESSAGE_HEADER:
                this.currentTable="messageheaders";
                this.appendToTable( columnName, value);
                break;
            case GROUP_HEADER:
                this.currentTable="groupheaders";
                this.appendToTable(columnName, value);
                break;
            case GROUP_ENTRIES:
                this.currentTable=scopeTracker.getNonTerminalScope();
    //            this.appendToResidual("GroupEntry\n");
                this.appendToTable( columnName,value);
                break;
            case UNKNOWN:
                break;
        }
    }

    public void close() throws IOException {
        for(SingleTableOutput singleTableOutput: singleTablesOutput.values()){
           singleTableOutput.close();
        }
    }
    public void startMessageHeader(){
        this.scopeTracker.scopeLevel= MESSAGE_HEADER;
    }

    public void endMessageHeader() throws IOException {
        this.singleTablesOutput.get("messageheaders").completeRow();
        this.scopeTracker.scopeLevel= UNKNOWN;
    }

    public void beginGroupHeader(){
        this.scopeTracker.scopeLevel= GROUP_HEADER;
    }
    public void endGroupHeader() throws IOException {
        this.scopeTracker.scopeLevel= UNKNOWN;
        this.singleTablesOutput.get("groupheaders").completeRow();
    }

    public void beginGroup() throws IOException {
       this.addTable(this.scopeTracker.getNonTerminalScope());
       this.scopeTracker.scopeLevel=ScopeLevel.GROUP_ENTRIES;
    }

    public void endGroup() throws IOException {
        this.singleTablesOutput.get(this.currentTable).completeRow();
        this.scopeTracker.clearAllButID();
    }

    public void beginPacketHeader() {
        this.scopeTracker.scopeLevel= PACKET_HEADER;

    }

    public void endPacketHeader() {
        this.scopeTracker.scopeLevel= UNKNOWN;
    }
}
