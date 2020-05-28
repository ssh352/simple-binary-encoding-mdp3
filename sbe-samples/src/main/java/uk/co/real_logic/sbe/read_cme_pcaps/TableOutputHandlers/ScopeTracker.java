package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class ScopeTracker {

    private final Deque<String> nonTerminalScope = new ArrayDeque<>();
    private ScopeLevel scopeLevel;

    public ScopeTracker(){
    }

    public void pushScope(String name) {
        this.nonTerminalScope.push (name);
    }

    public void popScope() {
        this.nonTerminalScope.pop();
    }

    void setScopeLevel(ScopeLevel scopeLevel){
        this.scopeLevel = scopeLevel;
    }


    public String getNonTerminalScope(){
        StringBuilder sb = new StringBuilder();
        final Iterator<String> i = nonTerminalScope.descendingIterator();
        sb.append(i.next());
        if(i.hasNext()){
        sb.append(".").append(i.next());
        }

        return sb.toString();
    }

    public void clearAllButID(){
        while(nonTerminalScope.size()>1){
            nonTerminalScope.pop();
        }
    }


    public void newToken(String tokenName) {
        nonTerminalScope.clear();
        this.pushScope(tokenName);
    }

    String getCurrentTable() {
        String currentTable = null;
        switch (this.scopeLevel) {
            case PACKET_HEADER:
                currentTable = "packetheaders";
                break;
            case MESSAGE_HEADER:
                currentTable = "messageheaders";
                break;
            case GROUP_HEADER:
                currentTable = "groupheaders";
                break;
            case GROUP_ENTRIES:
                //todo: see if there is more efficent way to keep track of tables than strings
                currentTable = this.getNonTerminalScope();
                //            this.appendToResidual("GroupEntry\n");
                break;
            case UNKNOWN:
                throw new IllegalStateException("Unexpected value: " + this.scopeLevel);
        }
        return currentTable;
    }
}
