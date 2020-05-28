package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.counters.CounterTypes;
import uk.co.real_logic.sbe.read_cme_pcaps.counters.RowCounter;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.io.IOException;
import java.util.HashMap;

import static uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel.*;

public class TablesHandler {
    final HashMap<String, SingleTableOutput> singleTablesOutput = new HashMap<>();
    final String path;
    private static final ScopeTracker scopeTracker = new ScopeTracker();
    RowCounter rowCounter;


    public TablesHandler(String path) throws IOException {
        this.path = path;
        this.rowCounter=new RowCounter();
        this.addTable("packetheaders");
        this.addTable("messageheaders");
        this.addTable("groupheaders");
    }

    public void addTable(String tableName) throws IOException {
        if (!singleTablesOutput.containsKey(tableName)) {
            SingleTableOutput newTableOutput = new SingleTableOutput(this.path, tableName, this.rowCounter);
            singleTablesOutput.put(tableName, newTableOutput);
        }
    }

    public void completeRow(String tableName) throws IOException {
        singleTablesOutput.get(tableName).completeRow();
        scopeTracker.setScopeLevel(UNKNOWN);
    }

   public void appendColumnValue(String columnName, Object value){
       this.appendColumnValue(columnName, String.valueOf(value) );
   }

    public void appendColumnValue(String columnName, String value) {
        this.singleTablesOutput.get(currentTable()).append(columnName, value);
    }

    public void close() throws IOException {
        for (SingleTableOutput singleTableOutput : singleTablesOutput.values()) {
            singleTableOutput.close();
        }
        this.singleTablesOutput.clear();
    }

    public void startMessageHeader(String tokenName, int tokenId) {
        //todo: it seams like not all message headers are alike..
        //separate into common message headers, and per message type header
        this.scopeTracker.newToken(tokenName);
        //todo: somehow enforce that begin entry happens after scope change.
        //perhaps separate each start into cope changes, row headers
        //perhaps have begin entry with an argument for entry type.. have a case switch
        this.newTableEntry(MESSAGE_HEADER);
        this.appendColumnValue("MessageId", tokenId);
        this.appendColumnValue("MessageName", tokenName);
    }

    public void endMessageHeader(String tokenName) throws IOException {
        this.scopeTracker.pushScope(tokenName);
        this.singleTablesOutput.get("messageheaders").completeRow();
    }


    public void beginGroupHeader() {
        this.newTableEntry(GROUP_HEADER);
    }

    public void endGroupHeader() throws IOException {
        this.singleTablesOutput.get("groupheaders").completeRow();
    }

    public void beginGroup(String tokenName) throws IOException {
        this.scopeTracker.pushScope(tokenName);
        this.addTable(this.scopeTracker.getNonTerminalScope());
        this.newTableEntry(GROUP_ENTRIES);
    }

    public void endGroup() throws IOException {
        this.singleTablesOutput.get(currentTable()).completeRow();
        this.scopeTracker.clearAllButID();
    }

    private void newTableEntry(ScopeLevel scopeLevel){
        this.scopeTracker.setScopeLevel(scopeLevel);
        this.beginEntry();
    }

    public void onBeginPacket(int message_size, long packet_sequence_number, long sendingTime) throws IOException {
        this.newTableEntry(PACKET_HEADER);
        this.appendColumnValue("message_size", message_size);
        this.appendColumnValue("packet_sequence_number", packet_sequence_number);
        this.appendColumnValue("sendingTime", sendingTime);
        this.completeRow("packetheaders");
    }

   private void beginEntry(){
        //todo: figure out how to do this at beginning of beginMessage/groupheader/groupelement
       // without manunually inserting it on each one
       //todo: see if there ar other common elements that can bet put into this. ScopeLevel?
        this.rowCounter.increment_count(CounterTypes.EVENT_COUNT);
        this.appendColumnValue("EntryCount", this.rowCounter.get_count(CounterTypes.EVENT_COUNT));
   }

    public void pushScope(String name) {
        this.scopeTracker.pushScope(name);
    }

    public void popScope() {
        this.scopeTracker.popScope();
    }

    private String currentTable(){
        return scopeTracker.getCurrentTable();
    }
}
