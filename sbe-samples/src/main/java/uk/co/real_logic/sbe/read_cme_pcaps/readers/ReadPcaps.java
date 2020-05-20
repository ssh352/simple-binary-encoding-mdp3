package uk.co.real_logic.sbe.read_cme_pcaps.readers;


import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CleanTokenListener;
import uk.co.real_logic.sbe.tests.DirectoryComparison;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class ReadPcaps {
    //    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;

    public static void main(final String[] args) throws Exception {
//        ReadPcapProperties prop = new ReadPcapProperties(args[0]);
        ReadPcapProperties prop = new ReadPcapProperties("C:\\marketdata\\testdata\\configs\\cleanlistener.config");
        boolean compareToPreviousFiles = false;

        BinaryDataHandler binaryDataHandler = new BinaryDataHandler(prop);

        TablesHandler tablesHandler = new TablesHandler("C:\\marketdata\\testdata\\separatetables\\latestresults\\");
        TokenListener cleanTokenListener = new CleanTokenListener(tablesHandler);

        PacketReader packetReader =new PacketReader(prop, binaryDataHandler);
        packetReader.readPackets(cleanTokenListener, tablesHandler);

        //test directory comparison by comparing same directory
        testRunEquality();
    }

    private static void testRunEquality() throws IOException {
        DirectoryComparison.compareDirectories("C:/marketdata/testdata/separatetables/referencedirectory/", "C:/marketdata/testdata/separatetables/latestresults/");
/*        if (compareToPreviousFiles) {
            String reference_file="c:/marketdata/testdata/separatetables/residualoutput_5_27.txt";
            String latest_output = "c:/marketdata/testdata/separatetables/residualoutput.txt";
            compare_files(reference_file, latest_output);
        }

 */
    }

}

