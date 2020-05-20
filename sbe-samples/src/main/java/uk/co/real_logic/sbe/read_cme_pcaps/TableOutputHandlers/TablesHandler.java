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


    public TablesHandler(String path) throws IOException {
        this.path=path;
        this.scopeTracker=new ScopeTracker();
        this.addTable("packetheaders");
        this.addTable("messageheaders");
        this.addTable("groupheaders");
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
    public void startMessageHeader(String tokenName){
        this.scopeTracker.clear();
        this.scopeTracker.pushScope(tokenName);
        this.scopeTracker.scopeLevel= MESSAGE_HEADER;
    }

    public void endMessageHeader(String tokenName) throws IOException {
        this.scopeTracker.pushScope(tokenName);
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

    public void beginGroup(String tokenName) throws IOException {
       this.scopeTracker.pushScope(tokenName);
       this.addTable(this.scopeTracker.getNonTerminalScope());
       this.scopeTracker.scopeLevel=ScopeLevel.GROUP_ENTRIES;
    }

    public void endGroup() throws IOException {
        this.singleTablesOutput.get(this.currentTable).completeRow();
        this.scopeTracker.clearAllButID();
    }

    public void setPacketValues(int bufferOffset, int message_size, long packet_sequence_number, long sendingTime) throws IOException {
        this.scopeTracker.scopeLevel= PACKET_HEADER;
        this.appendColumnValue("message_size", String.valueOf(message_size));
        this.appendColumnValue("packet_sequence_number",  String.valueOf(packet_sequence_number));
        this.appendColumnValue("sendingTime", String.valueOf(sendingTime));
        this.appendColumnValue(" bufferOffset", String.valueOf(bufferOffset));
        this.appendColumnValue("next_offset", String.valueOf(message_size));
        this.completeRow("packetheaders");
        this.scopeTracker.scopeLevel= UNKNOWN;
    }

   public void pushScope(String name) {
        this.scopeTracker.pushScope(name);
   }

    public void popScope() {
        this.scopeTracker.popScope();
    }
}
