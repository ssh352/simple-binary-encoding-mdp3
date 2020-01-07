package uk.co.real_logic.sbe.read_cme_pcaps;

import org.agrona.concurrent.UnsafeBuffer;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class ReadIcePcaps {
    //byte[] someArray = new byte[] { ... };

    public static <FileStreamReader> void main(final String[] args) throws Exception {
    final int SCHEMA_BUFFER_CAPACITY = 1000 * 1024;
    int messagesToRead=100;
    final ByteBuffer pcapDataBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);

    String binary_file_path = "c:/marketdata/ice_data/test_data/20191007.070000.080000.CME_GBX.CBOT.32_70.B.02.pcap.00014/20191007.070000.080000.CME_GBX.CBOT.32_70.B.02.pcap";

    RandomAccessFile aFile = new RandomAccessFile(binary_file_path, "rw");
    FileChannel inChannel = aFile.getChannel();
    MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
//    encodedMsgBuffer.flip();  //make buffer ready for read

    //ByteBufferKaitaiStream kaitaiStream = new ByteBufferKaitaiStream(pcapDataBuffer);

    ByteBufferKaitaiStream kaitaiFileStream = new ByteBufferKaitaiStream(binary_file_path);
    System.out.println(kaitaiFileStream.size());

//    System.out.println(kaitaiFileStream.pos());
//    byte[] bytes = kaitaiFileStream.readBytes(100);
//    System.out.println(bytes);
    Pcap data = new Pcap(kaitaiFileStream);
    System.out.println(data.hdr());

        int messagesRead=0;
    while (messagesRead < messagesToRead){

    //    Pcap data = new Pcap(new ByteBufferKaitaiStream(pcapDataBuffer));
        System.out.println(data.hdr());
    }




}}
