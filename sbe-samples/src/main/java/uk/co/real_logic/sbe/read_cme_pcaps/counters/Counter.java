package uk.co.real_logic.sbe.read_cme_pcaps.counters;

public class Counter {

    private int count;

    public Counter() {
        this.count = 0;
    }

    public void increment_count() {
        this.count++;
    }

    public int get_count() {
        return this.count;
    }

    public void reset_count() {
        this.count = 0;
    }
}
