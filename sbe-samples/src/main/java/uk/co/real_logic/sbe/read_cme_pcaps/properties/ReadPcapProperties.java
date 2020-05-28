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
    private final Properties prop = new Properties();

    public ReadPcapProperties(String fileName) throws IOException {
        InputStream is = new FileInputStream(fileName);
        this.prop.load(is);

        this.osString = getProperty("reader.os");
        this.outFile = this.getPath("reader.outFile");

        this.schemaFile = this.getPath("reader.schemaFile");
        this.runShort = Boolean.getBoolean(getProperty("reader.runShort"));
        String writeToFileString = getProperty("reader.writeToFile");

        this.writeToFile = Boolean.parseBoolean(writeToFileString);

        String in_files_string = getProperty("inputSources.inFiles");
        String[] inFileStringsTemp = in_files_string.split(",");
        ArrayList<String> inFiles = new ArrayList<>();
        for (String inFileString : inFileStringsTemp) {
            String path = Paths.get(inFileString).toString();
            inFiles.add(path);
        }
        this.inFiles = inFiles;

        this.dataSourceType = getProperty("reader.dataSourceType");
    }

    private String getProperty(String property) {
        return this.prop.getProperty(property);
    }

    private String getPath(String pathName){
        return Paths.get(getProperty(pathName)).toString();

    }
}