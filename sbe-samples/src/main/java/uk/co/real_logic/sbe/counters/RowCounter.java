package uk.co.real_logic.sbe.counters;

public class RowCounter {
    private int group_element_count;
    private int message_count;
    private int group_header_count;

    public RowCounter() {
        this.message_count = 0;
        this.group_header_count = 0;
        this.group_element_count = 0;
    }

    public void increment_message_count() {
        this.message_count++;
    }

    public int get_message_count() {
        return this.message_count;
    }

    public void increment_group_header_count() {
        this.group_header_count++;
    }

    public int get_group_header_count() {
        return this.group_header_count;
    }

    public void reset_group_header_count() {
        this.group_header_count = 0;
    }

    public void increment_group_element_count() {
        this.group_element_count++;
    }

    public int get_group_element_count() {
        return this.group_element_count;
    }

    public void reset_group_element_count() {
        this.group_element_count = 0;
    }

}

