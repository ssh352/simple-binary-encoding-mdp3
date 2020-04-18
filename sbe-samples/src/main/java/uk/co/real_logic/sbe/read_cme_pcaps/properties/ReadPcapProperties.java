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
    public final int max_buffers_to_process_short;

    public ReadPcapProperties(String fileName) {
        Properties prop = new Properties();
        // the configuration file name
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } //todo: put filename into somewhere easier to test (read in gradle?)  Separate file?
        // load the properties file
        try {
            prop.load(is);
            System.out.println(prop.getProperty("reader.name"));
            // get the value for app.version key
            System.out.println(prop.getProperty("reader.version"));
        } catch (IOException ex) {

        }
        os_string = prop.getProperty("reader.os");
        in_file = Paths.get(prop.getProperty("reader.in_file")).toString();
        out_file = Paths.get(prop.getProperty("reader.out_file")).toString();

        schema_file=Paths.get(prop.getProperty("reader.schema_file")).toString();
        String run_short_string = prop.getProperty("reader.run_short");
        run_short = Boolean.parseBoolean(run_short_string);
        max_buffers_to_process_short = Integer.parseInt(prop.getProperty("reader.max_buffers_to_process_short"));
        String write_to_file_string=prop.getProperty("reader.write_to_file");

        write_to_file = Boolean.parseBoolean(write_to_file_string);
        data_source = prop.getProperty("reader.data_source");
    }
}