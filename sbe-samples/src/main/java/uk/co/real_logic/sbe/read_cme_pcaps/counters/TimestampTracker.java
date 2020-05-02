package uk.co.real_logic.sbe.read_cme_pcaps.counters;

public class TimestampTracker {
    String sending_time;
    String transact_time;
    boolean sending_time_set;
    boolean transact_time_set = false;

    public TimestampTracker() {
    }

    public String getSending_time() {
        return this.sending_time;
    }

    public void setSending_time(long sending_time) {
        this.sending_time = String.valueOf(sending_time);
        this.sending_time_set = true;
    }

    public String getTransact_time() {
        return this.transact_time;
    }

    public void setTransact_time(String transact_time) {
        this.transact_time_set = true;
        this.transact_time = transact_time;
    }

    public Boolean transactTimeSet() {
        return transact_time_set;
    }

}
