import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

/**
 * @author naveen-5141
 */

public class Configuration {

    public String PROTOCOL;
    public String HOST;
    public String PORT;
    public String OUTPUT_DIR;
    public String URL_FILE;
    public String BUILD_HOME;
    public Boolean LOG_URL = false;
    public Boolean PERSIST_SQL_FILES = false;
    public Boolean COMPARE_RESPONSE = false;
    public Boolean COMPARE_SQL = false;
    public Boolean EXECUTE_URL = false;
    public Boolean APPLY_REGEX = false;
    public String ROOT;
    public JSONArray excludesList;
    public JSONArray replaceList;
    public Long WAIT_TIME = 0L;

    private static Configuration configuration = null;

    /**
     *
     * @return
     */
    public static Configuration loadConfiguration() {
        if (configuration == null) {
            configuration = new Configuration();
        }
        return configuration;
    }

    /**
     * Loads configuration files
     */
    public Configuration() {
        Properties props = new Properties();
        try {
            FileInputStream inStream = new FileInputStream(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator + "conf.properties");
            props.load(inStream);
            PROTOCOL = props.getProperty("PROTOCOL") + "://";
            HOST = props.getProperty("HOST");
            PORT = props.getProperty("PORT");
            BUILD_HOME = props.getProperty("BUILD_HOME");
            ROOT = BUILD_HOME + File.separator + "webapps" + File.separator + "ROOT";
            OUTPUT_DIR = props.getProperty("OUTPUT_DIR");
            URL_FILE = props.getProperty("URL_FILE");
            if (props.getProperty("log_url").equalsIgnoreCase("true")) {
                LOG_URL = true;
            }
            if (props.getProperty("persist_sql_files_in_backup").equalsIgnoreCase("true")) {
                PERSIST_SQL_FILES = true;
            }
            if (props.getProperty("compare_response_with_master").equalsIgnoreCase("true")) {
                COMPARE_RESPONSE = true;
            }
            if (props.getProperty("compare_sql_with_master").equalsIgnoreCase("true")) {
                COMPARE_SQL = true;
            }
            if (props.getProperty("execute_url").equalsIgnoreCase("true")) {
                EXECUTE_URL = true;
            }
            if (props.getProperty("apply_regex_while_compare").equalsIgnoreCase("true")) {
                APPLY_REGEX = true;
            }
            if (props.containsKey("wait_between_requests") && props.getProperty("wait_between_requests").equalsIgnoreCase("true")) {
                try {
                    WAIT_TIME = Long.parseLong(props.getProperty("wait_between_requests"));
                } catch(Exception e){
                    WAIT_TIME = 0L;
                }
            }
            new File(OUTPUT_DIR).mkdirs();
            new File(OUTPUT_DIR + File.separator + "sql").mkdirs();

            BufferedReader config = new BufferedReader(new FileReader(Paths.get(".").toAbsolutePath().normalize().toString() + File.separator + "config.json"));
            StringBuilder result = new StringBuilder();
            String configline;
            while ((configline = config.readLine()) != null) {
                result.append(configline);
            }
            JSONObject configurations = new JSONObject(result.toString());
            excludesList = configurations.getJSONArray("excludes");
            replaceList = configurations.getJSONArray("replace");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static Logger getLogger(String className) {
        try {
            Logger logger = Logger.getLogger(className);
            for (Handler handler : logger.getHandlers()) {
                handler.close();
                logger.removeHandler(handler);
            }
            Handler handler = null;
            handler = new FileHandler("log.txt", true);
            handler.setFormatter(new LogFormatter());
            logger.addHandler(handler);
            logger.setUseParentHandlers(false);
            return logger;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
