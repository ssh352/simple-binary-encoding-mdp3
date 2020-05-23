package uk.co.real_logic.sbe.read_cme_pcaps.readers;


import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CleanTokenListener;
import uk.co.real_logic.sbe.tests.DirectoryComparison;

import java.io.*;
import java.util.ArrayList;


public class ReadPcaps {
    //    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;

    public static void main(final String[] args) throws Exception {
//        ReadPcapProperties prop = new ReadPcapProperties(args[0]);
        String sourceConfig= "C:\\marketdata\\testdata\\configs\\multiplefiles.config";
        //todo: make multiple properties objects for different types of properties
        //todo: use different prefixes for these different objects. Pass only what is needed
        ReadPcapProperties prop = new ReadPcapProperties(sourceConfig);
        ArrayList<String> inFiles = prop.inFiles;

        for(String inFile: inFiles) {
            runJob(prop, inFile);
        }
    }

    private static void runJob(ReadPcapProperties prop, String inFile) throws Exception {

        BinaryDataHandler binaryDataHandler = new BinaryDataHandler(prop, inFile);

        TablesHandler tablesHandler = new TablesHandler("C:\\marketdata\\testdata\\separatetables\\latestresults\\");
        TokenListener cleanTokenListener = new CleanTokenListener(tablesHandler);

        PacketReader packetReader =new PacketReader(prop, binaryDataHandler, tablesHandler);
        packetReader.readPackets(cleanTokenListener);
        packetReader.endPacketsCollection();
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

