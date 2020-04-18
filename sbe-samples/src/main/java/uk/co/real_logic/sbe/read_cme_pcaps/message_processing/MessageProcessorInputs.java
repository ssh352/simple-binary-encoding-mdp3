package uk.co.real_logic.sbe.read_cme_pcaps.message_processing;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.IrEncoder;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.DataOffsets;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.stream_managers.PcapBufferManager;
import uk.co.real_logic.sbe.read_cme_pcaps.tablebulders.RowCounter;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MessageProcessorInputs {
    private static final int SCHEMA_BUFFER_CAPACITY = Integer.MAX_VALUE;

    public final RowCounter row_counter;
    public final OtfHeaderDecoder header_decoder;
    public final Ir ir;
    public final PcapBufferManager pcap_buffer_manager;
    private ReadPcapProperties prop;

    public MessageProcessorInputs(ReadPcapProperties prop) throws Exception {
        this.prop = prop;
        this.row_counter = new RowCounter();
        this.ir = getIr();
        this.header_decoder = new OtfHeaderDecoder(ir.headerStructure());
        this.pcap_buffer_manager = initializeBufferManager();
    }

    private PcapBufferManager initializeBufferManager() throws IOException {
        DataOffsets offsets = new DataOffsets(prop);
        RandomAccessFile aFile = new RandomAccessFile(this.prop.in_file, "rw");
        FileChannel inChannel = aFile.getChannel();
        MappedByteBuffer encodedMsgBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
        encodedMsgBuffer.flip();  //make buffer ready for read
        UnsafeBuffer buffer = new UnsafeBuffer(encodedMsgBuffer);
        PcapBufferManager bufferManager = new PcapBufferManager(prop, buffer);
        bufferManager.setBufferOffset(offsets.starting_offset); //skip leading bytes before message capture proper
        return bufferManager;
    }

    private Ir getIr() throws Exception {

        ByteBuffer encodedSchemaBuffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
        String schema_file = this.prop.schema_file;
        encodeSchema(encodedSchemaBuffer, schema_file);
        encodedSchemaBuffer.flip();
        return decodeIr(encodedSchemaBuffer);
    }


    private static void encodeSchema(ByteBuffer byteBuffer, final String schema_file) throws Exception {
        final File initialFile = new File(schema_file);
        final InputStream targetStream = new FileInputStream(initialFile);
        try (final InputStream in = new BufferedInputStream(targetStream)) {
            MessageSchema schema = XmlSchemaParser.parse(in, ParserOptions.DEFAULT);
            Ir ir = new IrGenerator().generate(schema);
            try (final IrEncoder irEncoder = new IrEncoder(byteBuffer, ir)) {
                irEncoder.encode();
            }
        }
    }


    private static Ir decodeIr(ByteBuffer buffer) {
        try (final IrDecoder irDecoder = new IrDecoder(buffer)) {
            return irDecoder.decode();
        }
    }


}
