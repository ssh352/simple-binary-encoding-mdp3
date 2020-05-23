package uk.co.real_logic.sbe.read_cme_pcaps.properties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

public class ReadPcapProperties {
    public final String osString;
    public ArrayList<String> inFiles;
    public final String outFile;
    public final String dataSourceType;
    public final String schemaFile;
    public final boolean runShort;
    public final boolean writeToFile;


    public ReadPcapProperties(String fileName) {
        Properties prop = new Properties();
        // the configuration file name
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        // load the properties file
        try {
            prop.load(is);
            System.out.println(prop.getProperty("reader.name"));
            // get the value for app.version key
            System.out.println(prop.getProperty("reader.version"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        this.osString = prop.getProperty("reader.os");
        this.outFile = Paths.get(prop.getProperty("reader.outFile")).toString();

        this.schemaFile = Paths.get(prop.getProperty("reader.schemaFile")).toString();
        this.runShort = Boolean.getBoolean(prop.getProperty("reader.runShort"));
        String writeToFileString = prop.getProperty("reader.writeToFile");

        this.writeToFile = Boolean.parseBoolean(writeToFileString);

        String in_files_string = prop.getProperty("inputSources.inFiles");
        String[] inFileStringsTemp =  in_files_string.split(",");
        ArrayList<String> inFiles = new ArrayList<>();
        for(String inFileString: inFileStringsTemp){
            String path =Paths.get(inFileString).toString();
           inFiles.add(path);
        }
        this.inFiles =inFiles;

        this.dataSourceType = prop.getProperty("reader.dataSourceType");
    }
}