package uk.co.real_logic.sbe.read_cme_pcaps.counters;


import java.util.HashMap;

public class RowCounter {
    private final HashMap<CounterTypes, Counter> row_counters= new HashMap<>();

    public RowCounter() {
        this.addCounter(CounterTypes.MESSAGE_COUNT);
        this.addCounter(CounterTypes.GROUP_HEADER_COUNT);
        this.addCounter(CounterTypes.GROUP_ELEMENT_COUNT);
        this.addCounter(CounterTypes.EVENT_COUNT);
    }

    public void increment_count(CounterTypes counter_type) {
        this.row_counters.get(counter_type).increment_count();
    }

    public int get_count(CounterTypes counter_type) {
        return this.row_counters.get(counter_type).get_count();
    }

    public void reset_count(CounterTypes counter_type) {
        this.row_counters.get(counter_type).reset_count();
    }

    public void onBeginMessage() {
        this.reset_count(CounterTypes.GROUP_ELEMENT_COUNT);
        this.reset_count(CounterTypes.GROUP_HEADER_COUNT);
    }

    public void onGroupHeader() {
        this.reset_count(CounterTypes.GROUP_ELEMENT_COUNT);
        this.increment_count(CounterTypes.GROUP_HEADER_COUNT);
    }

    public void onBeginGroup() {
        this.increment_count(CounterTypes.GROUP_ELEMENT_COUNT);
    }

    public void onBitSetEnd() {
        this.increment_count(CounterTypes.EVENT_COUNT);
    }

    private void addCounter(CounterTypes counterType){
        this.row_counters.put(counterType, new Counter());
    }
}

