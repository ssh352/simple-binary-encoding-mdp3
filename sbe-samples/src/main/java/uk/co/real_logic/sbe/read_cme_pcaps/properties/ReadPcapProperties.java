package uk.co.real_logic.sbe.read_cme_pcaps.properties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

public class ReadPcapProperties {
    public final String os_string;
    public final String in_file;
    public final String out_file;
    public final String data_source;
    public final String schema_file;
    public final boolean run_short;
    public final boolean write_to_file;


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
        this.os_string = prop.getProperty("reader.os");
        this.out_file = Paths.get(prop.getProperty("reader.out_file")).toString();

        this.schema_file = Paths.get(prop.getProperty("reader.schema_file")).toString();
        this.run_short = Boolean.getBoolean(prop.getProperty("reader.run_short"));
        String write_to_file_string = prop.getProperty("reader.write_to_file");

        this.write_to_file = Boolean.parseBoolean(write_to_file_string);
        this.data_source = prop.getProperty("reader.data_source");
        this.in_file = Paths.get(prop.getProperty("reader.in_file")).toString();

    }
}