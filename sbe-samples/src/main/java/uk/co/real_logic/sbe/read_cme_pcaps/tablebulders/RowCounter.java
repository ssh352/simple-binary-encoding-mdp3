package uk.co.real_logic.sbe.read_cme_pcaps.tablebulders;

public class RowCounter {
private int row_count;
private int packet_sequence_number;
private int template_id;

    public int getPacketSequenceNumber() {
        return packet_sequence_number;
    }

    public void setPacketSequenceNumber(int packet_sequence_number) {
        this.packet_sequence_number = packet_sequence_number;
    }

    public int getTemplateId() {
        return template_id;
    }

    public void setTemplateId(int template_id) {
        this.template_id = template_id;
    }


    public long getSending_time() {
        return sending_time;
    }

    public void setSending_time(long sending_time) {
        this.sending_time = sending_time;
    }

    private long sending_time;


    public RowCounter() {
        this.row_count = 0;
    }

    public int increment_row_count() {
        this.row_count+=1;
        return this.row_count;
    }


    public int get_row_count() {
        return this.row_count;
    }



}
