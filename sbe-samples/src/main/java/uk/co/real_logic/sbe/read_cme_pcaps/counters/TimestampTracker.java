package uk.co.real_logic.sbe.read_cme_pcaps.counters;

import static org.junit.Assert.assertFalse;

public class TimestampTracker {
    long sending_time;
    long transact_time;
    boolean sending_time_set;
    boolean transact_time_set = false;

    public TimestampTracker() {
    }

    public long getSending_time() {
        return this.sending_time;
    }

    public void setSending_time(long sending_time) {
        this.sending_time = sending_time;
        this.sending_time_set = true;
    }

    public long getTransact_time() {
        return this.transact_time;
    }

    public void setTransact_time(long transact_time) {
        assertFalse(transact_time_set);
        this.transact_time_set = true;
        this.transact_time = transact_time;
    }

    public Boolean transactTimeSet() {
        return transact_time_set;
    }

}
