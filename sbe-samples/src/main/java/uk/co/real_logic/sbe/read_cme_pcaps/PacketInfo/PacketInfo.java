package uk.co.real_logic.sbe.read_cme_pcaps.PacketInfo;

public class PacketInfo {
    final long packetSequenceNumber;
    final int templateID;

    public PacketInfo(int templateID, long packetSequenceNumber) {
        this.templateID = templateID;
        this.packetSequenceNumber = packetSequenceNumber;
    }

    public long getPacketSequenceNumber() {
        return packetSequenceNumber;
    }

    public int getTemplateID() {
        return templateID;
    }


}
