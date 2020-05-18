package uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers;

import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.ScopeLevel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class ScopeTracker {

    private final Deque<String> nonTerminalScope = new ArrayDeque<>();
    public ScopeLevel scopeLevel;
    public String scopeName;
    public ScopeTracker(){

    }

    public void pushScope(String name) {
        this.nonTerminalScope.push (name);
    }

    public void popScope() {
        this.nonTerminalScope.pop();
    }

    public ScopeLevel getCurrentScope(){
        return scopeLevel;
    }

    public String getCurrentScopeString() {
        StringBuilder sb = new StringBuilder();
        final Iterator<String> i = nonTerminalScope.descendingIterator();
        while (i.hasNext()) {
            sb.append(i.next());
            sb.append(".");
        }
        return sb.toString();
    }
}
