package uk.co.real_logic.sbe.read_cme_pcaps.properties;

import java.io.*;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

public class ReadPcapProperties {
    public Properties get_properties(String fileName) {
        Properties prop = new Properties();
            // the configuration file name
            InputStream is=null;
            try {
                is = new FileInputStream(fileName);
            }catch (FileNotFoundException ex){


            }
            // load the properties file
            try {
                prop.load(is);
                System.out.println(prop.getProperty("reader.name"));
                // get the value for app.version key
                System.out.println(prop.getProperty("reader.version"));
            }
            catch (IOException ex) {

            }
            return prop;
    }
}