package uk.co.real_logic.sbe.read_cme_pcaps.readers;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class BinaryDataHandler {


    private static final int SCHEMA_BUFFER_CAPACITY = 50000 * 1024;

    private final Ir ir;
    private final OtfHeaderDecoder headerDecoder;
    private final UnsafeBuffer buffer;

    public BinaryDataHandler(ReadPcapProperties prop, String in_file) throws Exception {

        final ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
        EncoderDecoders.encodeSchema(encodedSchemaBuffer, prop.schemaFile);
        RandomAccessFile aFile = new RandomAccessFile(in_file, "rw");
        FileChannel inChannel = aFile.getChannel();


        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
        encodedMsgBuffer.flip();  //make buffer ready for read

        // Now lets decode the schema IR so we have IR objects.
        encodedSchemaBuffer.flip();
        ir = EncoderDecoders.decodeIr(encodedSchemaBuffer);
        headerDecoder = new OtfHeaderDecoder(ir.headerStructure());
        buffer = new UnsafeBuffer(encodedMsgBuffer);
    }


    public Ir getIr() {
        return ir;
    }

    public OtfHeaderDecoder getHeaderDecoder() {
        return headerDecoder;
    }



    public UnsafeBuffer getBuffer() {
        return buffer;
    }

}