package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

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
        if(scopeTracker.getCurrentScope()!=ScopeLevel.UNKNOWN){
            if(scopeTracker.getCurrentScope()==ScopeLevel.GROUP_ENTRIES){
                this.appendToTable(this.scopeTracker.getCurrentScopeString(), columnName, value);
            } else{
                this.appendToTable(this.scopeTracker.scopeName, columnName, value);
            }
        }
        else{
            this.appendToResidual(columnName + "/" + value);
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
        this.scopeTracker.scopeLevel=ScopeLevel.MESSAGE_HEADER;
        this.scopeTracker.scopeName="messageheaders";
    }

    public void endMessageHeader() throws IOException {
        this.singleTablesOutput.get("messageheaders").completeRow();
        this.scopeTracker.scopeLevel=ScopeLevel.UNKNOWN;
        this.scopeTracker.scopeName="unknown";
    }

    public void beginGroupHeader(){
        this.scopeTracker.scopeName="groupheaders";
        this.scopeTracker.scopeLevel=ScopeLevel.GROUP_HEADER;
    }
    public void endGroupHeader() throws IOException {
        this.scopeTracker.scopeLevel=ScopeLevel.UNKNOWN;
        this.singleTablesOutput.get("groupheaders").completeRow();
        this.scopeTracker.scopeName="unknown";
    }

    public void beginGroup(String tokenName) throws IOException {
        this.appendToResidual("tableshandlerbegingroup " );
        this.appendToResidual(this.scopeTracker.getCurrentScopeString());
//       this.addTable(tokenName);

//       this.scopeTracker.scopeLevel=ScopeLevel.GROUP_ENTRIES;
//       this.scopeTracker.scopeName=tokenName;
    }
    public void endGroup(){
        this.appendToResidual("tableshandlerendgroup " );
//        this.scopeTracker.scopeLevel=ScopeLevel.UNKNOWN;
//        this.scopeTracker.scopeName="unknown";
    }

}
