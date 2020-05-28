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

//todo:  remove references to pushing, poping, clearing scope etc into ScopeTracker

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
        this.newEntry(MESSAGE_HEADER);
        this.appendColumnValue("MessageId", tokenId);
        this.appendColumnValue("MessageName", tokenName);
    }

    public void endMessageHeader() throws IOException {
//        this.scopeTracker.pushScope(tokenName);
        this.singleTablesOutput.get("messageheaders").completeRow();
    }


    public void beginGroupHeader(String tokenName) throws IOException {
        this.newEntry(GROUP_HEADER, tokenName);
    }

    public void endGroupHeader() throws IOException {
        this.singleTablesOutput.get("groupheaders").completeRow();
    }

    public void beginGroup(String tokenName) throws IOException {
        this.scopeTracker.pushScope(tokenName);
        System.out.println("tablenamesonbegingroup\n" + scopeTracker.getCurrentTable()+ "\n"+ scopeTracker.getNonTerminalScope());

        this.addTable(this.scopeTracker.getNonTerminalScope());
//        this.scopeTracker.setScopeLevel(GROUP_ENTRIES);
        this.newEntry(GROUP_ENTRIES);
    }

    public void endGroup() throws IOException {
        this.singleTablesOutput.get(currentTable()).completeRow();
        this.scopeTracker.clearAllButID();
    }


    private void newEntry(ScopeLevel scopeLevel, String newLabel) throws IOException {
        this.scopeTracker.pushScope(newLabel);
        this.newEntry(scopeLevel);
    }

    private void newEntry(ScopeLevel scopeLevel){
        this.scopeTracker.setScopeLevel(scopeLevel);
        this.rowCounter.increment_count(CounterTypes.EVENT_COUNT);
        this.appendColumnValue("EntryCount", this.rowCounter.get_count(CounterTypes.EVENT_COUNT));
    }

    public void onBeginPacket(int message_size, long packet_sequence_number, long sendingTime) throws IOException {
        this.newEntry(PACKET_HEADER);
        this.appendColumnValue("message_size", message_size);
        this.appendColumnValue("packet_sequence_number", packet_sequence_number);
        this.appendColumnValue("sendingTime", sendingTime);
        this.completeRow("packetheaders");
    }



    public void pushScope(String name) {
        this.scopeTracker.pushScope(name);
    }

    public void popScope() {
        this.scopeTracker.popScope();
    }

    private String currentTable(){
        System.out.println("tablenames\n" + scopeTracker.getCurrentTable()+ "\n"+ scopeTracker.getNonTerminalScope());
        return scopeTracker.getCurrentTable();
    }
}
