package uk.co.real_logic.sbe.read_cme_pcaps.PacketInfo;

public class PacketInfo {
    final long packetSequenceNumber;
    final int templateID;

    private boolean transactTimeSet=false;
    final String sendingTime;
    String transactTime;

    public PacketInfo(int templateID, long packetSequenceNumber, long sendingTime) {
        this.templateID = templateID;
        this.packetSequenceNumber = packetSequenceNumber;
        this.sendingTime = String.valueOf(sendingTime);
    }

    public void setTransactTime(String transactTime) {
        this.transactTime = transactTime;
        this.transactTimeSet=true;
    }

    public boolean transactTimeSet(){
        return this.transactTimeSet;
    }

    public long getPacketSequenceNumber() {
        return packetSequenceNumber;
    }

    public int getTemplateID() {
        return templateID;
    }

    public String getTransactTime() {
        return transactTime;
    }

    public String getSendingTime() {
        return sendingTime;
    }

}
