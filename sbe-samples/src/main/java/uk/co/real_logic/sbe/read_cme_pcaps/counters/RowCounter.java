package uk.co.real_logic.sbe.read_cme_pcaps.counters;


import java.util.HashMap;

public class RowCounter {
    private HashMap<CounterTypes, Counter> row_counters;


    public RowCounter() {
        this.row_counters = new HashMap<>();
        this.row_counters.put(CounterTypes.MESSAGE_COUNT, new Counter());
        this.row_counters.put(CounterTypes.GROUP_HEADER_COUNT, new Counter());
        this.row_counters.put(CounterTypes.GROUP_ELEMENT_COUNT, new Counter());
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

}
