package uk.co.real_logic.sbe.read_cme_pcaps.readers;


import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.read_cme_pcaps.TableOutputHandlers.TablesHandler;
import uk.co.real_logic.sbe.read_cme_pcaps.properties.ReadPcapProperties;
import uk.co.real_logic.sbe.read_cme_pcaps.token_listeners.CleanTokenListener;
import uk.co.real_logic.sbe.tests.DirectoryComparison;

import java.io.*;
import java.util.ArrayList;
import java.util.TreeMap;


public class ReadPcaps {
    //    private static final int MSG_BUFFER_CAPACITY = 1000000 * 1024;

    public static void main(final String[] args) throws Exception {
//        ReadPcapProperties prop = new ReadPcapProperties(args[0]);
        String sourceConfig = "C:\\marketdata\\testdata\\configs\\singlefile.config";
        //todo: make multiple properties objects for different types of properties
        //todo: use different prefixes for these different objects. Pass only what is needed
        ReadPcapProperties prop = new ReadPcapProperties(sourceConfig);


       ArrayList<String> inFiles=getFilesFromProperties(prop) ;
        //todo: figure out why loop doesn't work when stated with colon
//        String filesPath = "C:\\marketdata\\testdata\\prodreferencedata\\";
//        String filesPath = "C:\\marketdata\\testdata\\20191010\\";
//        inFiles=getFilesFromDirectoryName(filesPath);
        runJob(prop, inFiles);
//        testRunEquality();
    }



   private static ArrayList<String> getFilesFromProperties(ReadPcapProperties prop){
       return prop.inFiles;
   }

    private static ArrayList<String> getFilesFromDirectoryName(String filesPath){
//        File path = new File("C:\\marketdata\\testdata\\fulldaypcaps\\");
        File[] listFiles = new File(filesPath).listFiles();
        TreeMap orderedFilePaths = new TreeMap<Integer, String>();
        ArrayList<String>  fileStrings = new ArrayList<>();
        for (int i = 0; i < listFiles.length; i++){
            String fileString =  listFiles[i].toString();
            String[] splitString = fileString.split("[.]");
            int lastDigit= Integer.parseInt(splitString[splitString.length-1]);
            orderedFilePaths.put(lastDigit, fileString);
            }
        for (int i = 1; i <= listFiles.length; i++){
            fileStrings.add(String.valueOf(orderedFilePaths.get(i)));
        }

        return fileStrings;

    }


    private static void runJob(ReadPcapProperties prop, ArrayList<String> inFiles) throws Exception {

        TablesHandler tablesHandler = new TablesHandler("C:\\marketdata\\testdata\\separatetables\\latestresults\\");
        for (int i = 0; i < inFiles.size(); i++) {
            String inFile = inFiles.get(i);


            BinaryDataHandler binaryDataHandler = new BinaryDataHandler(prop, inFile);

            TokenListener cleanTokenListener = new CleanTokenListener(tablesHandler);

            PacketReader packetReader = new PacketReader(prop, binaryDataHandler, tablesHandler);
            packetReader.readPackets(cleanTokenListener);

        }
        tablesHandler.close();
        //test directory comparison by comparing same directory
        testRunEquality();
    }

    private static void testRunEquality() throws IOException {
//        DirectoryComparison.compareDirectories("C:/marketdata/testdata/separatetables/referencedirectory/", "C:/marketdata/testdata/separatetables/latestresults/");
        DirectoryComparison.compareDirectories("C:/marketdata/testdata/separatetables/singlefilereference/", "C:/marketdata/testdata/separatetables/latestresults/");
/*        if (compareToPreviousFiles) {
            String reference_file="c:/marketdata/testdata/separatetables/residualoutput_5_27.txt";
            String latest_output = "c:/marketdata/testdata/separatetables/residualoutput.txt";
            compare_files(reference_file, latest_output);
        }

 */
    }

}

