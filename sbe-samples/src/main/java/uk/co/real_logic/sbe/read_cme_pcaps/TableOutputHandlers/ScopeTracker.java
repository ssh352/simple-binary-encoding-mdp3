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


    public String getNonTerminalScope(){
        StringBuilder sb = new StringBuilder();
        final Iterator<String> i = nonTerminalScope.descendingIterator();
        sb.append(i.next());
        if(i.hasNext()){
        sb.append(".");
        sb.append(i.next());}
        else {
            int abc=1;
        }

        return sb.toString();
    }
    public ScopeLevel getScopeLevel(){
        return scopeLevel;
    }
/*
    public String getCurrentScopeString() {
        StringBuilder sb = new StringBuilder();
        final Iterator<String> i = nonTerminalScope.descendingIterator();
        while (i.hasNext()) {
            sb.append(i.next());
            if(i.hasNext()){
                sb.append(".");
            }
        }
        return sb.toString();
    }
*/
    public void clearAllButID(){
        while(nonTerminalScope.size()>1){
            nonTerminalScope.pop();
        }
    }

    public void clear() {

        nonTerminalScope.clear();
    }
}
