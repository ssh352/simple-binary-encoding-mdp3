package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

import static uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel.*;

public class TablesHandler {
    HashMap<String, SingleTableOutput> singleTablesOutput= new HashMap<>();
    String currentTable;
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

    private void appendToTable(String columnName, String value){
        singleTablesOutput.get(this.currentTable).append(columnName, value);
    }

    public  void completeRow(String tableName) throws IOException {
        singleTablesOutput.get(tableName).completeRow();
    };

    public void flush() throws IOException {
        this.residualOutput.flush();
    }



    public void appendToCurrentScope(String columnName, String value){
        if(value=="703408152454"){
            boolean unfilled_row=true;
        }
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
                this.appendToResidual("\n");
                this.appendToResidual("group entry current scope string: ");
                this.appendToResidual(scopeTracker.getCurrentScopeString());
                this.appendToResidual("\n");
                this.appendToResidual(columnName);
                this.appendToResidual("/");
                this.appendToResidual(value);
                this.appendToResidual("\n");
                this.appendToTable( columnName,value);
                break;
            case UNKNOWN:
                this.appendToResidual("Unknown\n");
                this.appendToResidual(columnName);
                this.appendToResidual("/");
                this.appendToResidual(value);
                this.appendToResidual("\n");
                break;
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
/*
    public void appendScope() {
        this.appendToResidual(scopeTracker.getCurrentScopeString());
    }
*/
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
        this.appendToResidual("currentScope: " + this.scopeTracker.getCurrentScopeString() + "/n");
       this.addTable(this.scopeTracker.getNonTerminalScope());
       this.scopeTracker.scopeLevel=ScopeLevel.GROUP_ENTRIES;
//       this.scopeTracker.scopeName=tokenName;
    }

    public void endGroup() throws IOException {
        this.appendToResidual("tableshandler\nendgroup\n " );
//        this.scopeTracker.scopeLevel=ScopeLevel.UNKNOWN;
        this.singleTablesOutput.get(this.currentTable).completeRow();
        this.scopeTracker.scopeName="unknown";
        this.scopeTracker.clearScope();
    }

    public void beginPacketHeader() {
        this.scopeTracker.scopeLevel= PACKET_HEADER;

    }

    public void endPacketHeader() {
        this.scopeTracker.scopeLevel= UNKNOWN;
    }
}
