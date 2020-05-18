package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

import static uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel.*;

public class TablesHandler {
    HashMap<String, SingleTableOutput> singleTablesOutput= new HashMap<>();
    String path;
    Writer residualOutput;
    private ScopeTracker scopeTracker;
    //todo: get rid of reference to explicit outwriter in token listener.. override append
    //eventually get rid of residualOutput
    public TablesHandler(String path, Writer residualOutput, ScopeTracker scopeTracker){
        this.path=path;
        this.residualOutput=residualOutput;
        this.scopeTracker=scopeTracker;
    }



    public void addTable( String tableName) throws IOException {
        if(!singleTablesOutput.containsKey(tableName)){
        SingleTableOutput newTableOutput= new SingleTableOutput(this.path, tableName);
        singleTablesOutput.put(tableName, newTableOutput) ;}
//        singleTablesOutput.put(tableName, new SingleTableOutput(this.path, tableName)) ;

    }

    public void appendToTable(String tableName, String columnName, String value){
        singleTablesOutput.get(tableName).append(columnName, value);
    }

    public  void completeRow(String tableName) throws IOException {
        singleTablesOutput.get(tableName).completeRow();
    };

    public void flush() throws IOException {
        this.residualOutput.flush();
    }

    public void appendToCurrentScope(String columnName, String value){
        switch(scopeTracker.getCurrentScope()){
            case PACKET_HEADER:
                this.appendToTable("packetheaders", columnName, value);
                break;
            case MESSAGE_HEADER:
                this.appendToTable("messageheaders", columnName, value);
                break;
            case GROUP_HEADER:
                this.appendToTable("groupheaders", columnName, value);
                break;
            case GROUP_ENTRIES:
                this.appendToResidual("GroupEntry\n");
                this.appendToResidual(columnName);
                this.appendToResidual("/");
                this.appendToResidual(value);
                this.appendToResidual("\n");
            case UNKNOWN:
                this.appendToResidual("Unknown\n");
                this.appendToResidual(columnName);
                this.appendToResidual("/");
                this.appendToResidual(value);
                this.appendToResidual("\n");
        }
    }

    public void appendToResidual(String value)  {
        try {
            this.residualOutput.append(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        this.residualOutput.close();
        for(SingleTableOutput singleTableOutput: singleTablesOutput.values()){
           singleTableOutput.close();
        }
    }

    public void appendScope() {
        this.appendToResidual(scopeTracker.getCurrentScopeString());
    }

    public void startMessageHeader(){
        this.scopeTracker.scopeLevel= MESSAGE_HEADER;
        this.scopeTracker.scopeName="messageheaders";
    }

    public void endMessageHeader() throws IOException {
        this.singleTablesOutput.get("messageheaders").completeRow();
        this.scopeTracker.scopeLevel= UNKNOWN;
        this.scopeTracker.scopeName="unknown";
    }

    public void beginGroupHeader(){
        this.scopeTracker.scopeName="groupheaders";
        this.scopeTracker.scopeLevel= GROUP_HEADER;
    }
    public void endGroupHeader() throws IOException {
        this.scopeTracker.scopeLevel= UNKNOWN;
        this.singleTablesOutput.get("groupheaders").completeRow();
        this.scopeTracker.scopeName="unknown";
    }

    public void beginGroup(String tokenName) throws IOException {
        this.appendToResidual("tableshandler\nbegingroup\n " );
        this.appendToResidual(this.scopeTracker.getCurrentScopeString());
//       this.addTable(tokenName);
//       this.scopeTracker.scopeLevel=ScopeLevel.GROUP_ENTRIES;
//       this.scopeTracker.scopeName=tokenName;
    }

    public void endGroup(){
        this.appendToResidual("tableshandler\nendgroup\n " );
//        this.scopeTracker.scopeLevel=ScopeLevel.UNKNOWN;
//        this.scopeTracker.scopeName="unknown";
    }

    public void beginPacketHeader() {
        this.scopeTracker.scopeLevel= PACKET_HEADER;

    }

    public void endPacketHeader() {
        this.scopeTracker.scopeLevel= UNKNOWN;
    }
}
