package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;

public class PacketOffsets {
        private DataOffsets offsets;
        private int nextCaptureOffset;
        private int headerLength;
        private int captureOffset;
        private int packetOffset;
        private int headerStartOffset;
        private int messageOffset;

        public PacketOffsets(DataOffsets offsets, int nextCaptureOffset, int headerLength) {
            this.offsets = offsets;
            this.nextCaptureOffset = nextCaptureOffset;
            this.headerLength = headerLength;
        }

        public int getCaptureOffset() {
            return captureOffset;
        }

        public int getPacketOffset() {
            return packetOffset;
        }

        public int getHeaderStartOffset() {
            return headerStartOffset;
        }

        public int getMessageOffset() {
            return messageOffset;
        }

        public PacketOffsets invoke() {
            captureOffset = nextCaptureOffset;
            packetOffset = captureOffset;
            headerStartOffset = captureOffset + offsets.header_bytes;
            messageOffset = headerStartOffset + headerLength;
            return this;
        }
    }

