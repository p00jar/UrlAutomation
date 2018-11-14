import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONTokener;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author naveen-5141
 */

public class DiffUtil {

    JSONArray masterList = new JSONArray();

    JSONArray childList = new JSONArray();
    private static Configuration CONFIG;

    private static Logger logger = Configuration.getLogger(DiffUtil.class.getName());

    private static HashMap<String, String> map = new HashMap();

    private static List<String> report = new ArrayList();

    public DiffUtil() {
        CONFIG = Configuration.loadConfiguration();
    }

    public void init() throws IOException {
        if (CONFIG.COMPARE_RESPONSE) {
            gatherFiles();
            if (CONFIG.APPLY_REGEX) {
                applyRegex();
            }
            compareFiles();
        }
        if (CONFIG.COMPARE_SQL) {
            compareSQL();
        }
    }

    private void applyRegex() {
        logger.log(Level.INFO, "Going to apply regex");
        applyRegex("master", masterList);
        applyRegex("child", childList);
    }

    private void applyRegex(String type, JSONArray filesList) {
        try (ProgressBar pb = new ProgressBar("Applying regex to " + type, filesList.length(), ProgressBarStyle.ASCII)) {
            for (int iter = 0; iter < filesList.length(); iter++) {
                pb.step();
                String fileName = (String) filesList.getJSONObject(iter).get("File Name");
                String filePath = CONFIG.ROOT + File.separator + type + File.separator + fileName;
                String content = getModifiedContent(getFileContent(filePath));
                try {
                    Files.write(Paths.get(CONFIG.ROOT + File.separator + type + File.separator + fileName), content.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Apply regex to response content as configured in config.json.
     *
     * @param content
     * @return
     */
    private static String getModifiedContent(String content) {
        for (int i = 0; i < CONFIG.replaceList.length(); i++) {
            JSONObject config = CONFIG.replaceList.getJSONObject(i);
            String regexToFind = config.getString("find");
            String replaceWith = config.getString("replace");
            if (regexToFind != null && replaceWith != null) {
                content = content.replaceAll(regexToFind, replaceWith);
            }
        }

        return content;
    }


    /**
     * Compares sql files in master and child
     */
    private void compareSQL() {
        int count = 1;
        String mainDir = CONFIG.ROOT + File.separator + "master" + File.separator + "sql";
        String childDir = CONFIG.ROOT + File.separator + "child" + File.separator + "sql";
        new File(mainDir).mkdirs();
        File mastersqlFiles[] = new File(mainDir).listFiles();
        StringBuilder reportBuilder = new StringBuilder();
        reportBuilder.append("<html><head><script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js\"></script></head><style>.master td{background-color:#cce5cc;vertical-align:top;} .child td{background-color:#ffe5e5;vertical-align:top} table{border: 2px solid #ddd;}</style><body>");
        Properties diffFiles = new Properties();
        try (ProgressBar pb = new ProgressBar("Comparing DB", mastersqlFiles.length, ProgressBarStyle.ASCII)) {
            for (File masterFile : mastersqlFiles) {
                pb.step();
                String fileName = masterFile.getName();
                logger.log(Level.INFO, fileName);
                if (fileName.endsWith("sql") && !fileName.contains("uvhvalues")) {
                    File childFile = new File(childDir + File.separator + fileName);
                    StringBuilder builder = new StringBuilder();
                    List<String> original = fileToLines(masterFile.getAbsolutePath());
                    List<String> revised = fileToLines(childFile.getAbsolutePath());
                    Patch patch = DiffUtils.diff(original, revised);
                    if (patch.getDeltas().size() > 0) {
                        Map<Character, Integer> masterFreqencyMap = getFrequencyMap(getFileContent(masterFile.getAbsolutePath()));
                        Map<Character, Integer> childFrequencyMap = getFrequencyMap(Objects.requireNonNull(getFileContent(childFile.getAbsolutePath())));
                        if (!masterFreqencyMap.equals(childFrequencyMap)) {
                            reportBuilder.append("<h2>").append(masterFile.getName());
                            reportBuilder.append("<table cellspacing='0' cellpadding='5'>");
                            for (Object delta : patch.getDeltas()) {
                                Delta<String> del = (Delta<String>) delta;
                                builder.append("Master : ").append(del.getOriginal().toString().replaceAll("<", "&lt;").trim());
                                builder.append("Build  : ").append(del.getRevised().toString().replaceAll("<", "&lt;").trim());
                                builder.append("********");

                                reportBuilder.append("<tr class='master'><td>").append(del.getOriginal().getPosition() + 1).append("</td><td>").append(del.getOriginal().getLines().toString().replaceAll("<", "&lt;").trim()).append("</td></tr>");
                                reportBuilder.append("<tr class='child'><td>").append(del.getRevised().getPosition() + 1).append("</td><td>").append(del.getRevised().getLines().toString().replaceAll("<", "&lt;").trim()).append("</td></tr>");

                                reportBuilder.append("<tr><td colspan=2>&nbsp;</td></tr>");
                            }
                            reportBuilder.append("</table><br><br>");

                            try {
                                new File(CONFIG.ROOT + File.separator + "sqldiff").mkdirs();
                                new File(CONFIG.ROOT + File.separator + "sqldiff" + File.separator + masterFile.getName()).createNewFile();
                                FileWriter writer = new FileWriter(CONFIG.ROOT + File.separator + "sqldiff" + File.separator + masterFile.getName());
                                writer.write(builder.toString());
                                writer.close();
                                diffFiles.setProperty(masterFile.getName(), masterFile.getName());
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                }
            }
        }

        reportBuilder.append("</body></html>");
        try {
            FileWriter writer = new FileWriter(CONFIG.ROOT + File.separator + "sqldiff.html");
            writer.write(reportBuilder.toString());
            writer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Loads master and child files to be compared.
     */
    private void gatherFiles() {
        logger.log(Level.INFO, "Going to gather files");
        String mainDir = CONFIG.ROOT + File.separator + "master" + File.separator + "fileList.json";
        String childDir = CONFIG.ROOT + File.separator + "child" + File.separator + "fileList.json";

        try {
            FileInputStream inStream = new FileInputStream(mainDir);
            masterList = new JSONArray(new JSONTokener(inStream));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try {
            FileInputStream inStream = new FileInputStream(childDir);
            childList= new JSONArray(new JSONTokener(inStream));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.out.println("Files in Master : " + masterList.length());
        System.out.println("Files in Child  : " + childList.length());
    }

    /**
     * Compares html and json files loaded in master and child list.
     *
     * @throws IOException
     */
    private void compareFiles() throws IOException {
        Properties diffFiles = new Properties();
        StringBuilder reportBuilder = new StringBuilder();
        reportBuilder.append("<html><head><script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js\"></script></head><style>.master td{background-color:#cce5cc;vertical-align:top;} .child td{background-color:#ffe5e5;vertical-align:top} table{border: 2px solid #ddd;}</style>");
        reportBuilder.append("<script>function showLink(ele){ if (ele.parentElement.nextElementSibling.style.display === 'none') {ele.parentElement.nextElementSibling.style.display = 'block'; ele.textContent = 'hide url'} else {ele.parentElement.nextElementSibling.style.display = 'none'; ele.textContent = 'show url'}}</script><body>");
        try (ProgressBar pb = new ProgressBar("Comparing responses", masterList.length(), ProgressBarStyle.ASCII)) {
            for (int iter = 0; iter < masterList.length();iter++) {
                pb.step();
                String key = (String) masterList.getJSONObject(iter).get("File Name");
                String masterFile = CONFIG.ROOT + File.separator + "master" + File.separator + key;
                String childFile = CONFIG.ROOT + File.separator + "child" + File.separator + key;
                StringBuilder builder = new StringBuilder();
                if (key.endsWith("html") || key.endsWith("xml") || key.endsWith("txt")) {
                    fileDiff(reportBuilder, builder, masterFile, childFile, key,masterList.getJSONObject(iter));
                } else if (key.endsWith("json")) {
                    jsonDiff(reportBuilder, builder, masterFile, childFile, key,masterList.getJSONObject(iter));
                }
            }
        }

        try {
            new File(CONFIG.ROOT + File.separator + "diff").mkdirs();
            new File(CONFIG.ROOT + File.separator + "diff" + File.separator + "fileList.properties").createNewFile();
            FileOutputStream localOut = new FileOutputStream(CONFIG.ROOT + File.separator + "diff" + File.separator + "fileList.properties"); // No I18N
            diffFiles.store(localOut, "Files with Diff");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        reportBuilder.append("</body><script>$(document).ready(function(){");

        for(Object o : report){
            getKeyCount(reportBuilder,o);
        }
        reportBuilder.append("});</script></html>");
        try {
            FileWriter writer = new FileWriter(CONFIG.ROOT + File.separator + "diff.html");
            writer.write(reportBuilder.toString());
            writer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void jsonDiff(StringBuilder reportBuilder, StringBuilder builder, String masterFile, String childFile, String key,JSONObject masterObj) {
        String masterContent = getFileContent(masterFile);
        String childContent = getFileContent(childFile);
        StringBuilder diffString = new StringBuilder();
        try {
            JSONAssert.assertEquals(childContent, masterContent, false);
        } catch (AssertionError e) {
            if (masterContent != null && childContent != null) {
                diffString.append(masterContent).append(childContent);
                if(!map.containsValue(diffString.toString())){
                    reportBuilder.append("<h2>").append(key).append(" [<a href='javascript:void(0)' onclick='showLink(this)'>show url</a>]</h2><span style='display:none;word-wrap:break-word'>").append(masterObj.get("Operation")).append("##").append(masterObj.get("Login Name")).append("##").append(masterObj.get("URL")).append("</span>");
                    reportBuilder.append("<p id='").append(key.substring(0, key.indexOf('.'))).append("'></p>");
                reportBuilder.append("<table cellspacing='0' cellpadding='5'>");

                builder.append("Master : ").append(masterContent);
                builder.append("Build  : ").append(childContent);
                builder.append("********");

                reportBuilder.append("<tr class='master'><td>").append(1).append("</td><td>").append(masterContent.replaceAll("<", "&lt;").trim()).append("</td></tr>");
                reportBuilder.append("<tr class='child'><td>").append(1).append("</td><td>").append(childContent.replaceAll("<", "&lt;").trim()).append("</td></tr>");

                reportBuilder.append("<tr><td colspan=2>&nbsp;</td></tr>");
                reportBuilder.append("</table><br><br>");

                }
                else{
                    if(!report.contains(diffString.toString())){
                        report.add(diffString.toString());
                    }
                }
                map.put(key, diffString.toString());

                try {
                    new File(CONFIG.ROOT + File.separator + "diff").mkdirs();
                    new File(CONFIG.ROOT + File.separator + "diff" + File.separator + key).createNewFile();
                    FileWriter writer = new FileWriter(CONFIG.ROOT + File.separator + "diff" + File.separator + key);
                    writer.write(builder.toString());
                    writer.close();
                    //diffFiles.setProperty(key, masterList.getProperty(key));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch(JSONException e){
            fileDiff(reportBuilder, builder, masterFile, childFile, key,masterObj);
        }
    }

    private void fileDiff(StringBuilder reportBuilder,StringBuilder builder, String masterFile, String childFile, String key,JSONObject masterObj) {
        List<String> original = fileToLines(masterFile);
        List<String> revised = fileToLines(childFile);
        Patch patch = DiffUtils.diff(original, revised);
        StringBuilder diffString = new StringBuilder();
        if (patch.getDeltas().size() > 0) {
            Map<Character, Integer> masterFreqencyMap = getFrequencyMap(getFileContent(masterFile));
            Map<Character, Integer> childFrequencyMap = getFrequencyMap(getFileContent(childFile));
            if (!masterFreqencyMap.equals(childFrequencyMap)) {
                StringBuilder test = new StringBuilder();
                for (Object delta : patch.getDeltas()) {
                    Delta<String> del = (Delta<String>) delta;
                    test.append(del.getOriginal().getLines().toString()).append(del.getRevised().getLines().toString());
                }
                diffString.append(test);
                if(!map.containsValue(diffString.toString())){
                    reportBuilder.append("<h2>").append(key).append(" [<a href='javascript:void(0)' onclick='showLink(this)'>show url</a>]</h2><span style='display:none;word-wrap:break-word'>").append(masterObj.get("Operation")).append("##").append(masterObj.get("Login Name")).append("##").append(masterObj.get("URL")).append("</span>");
                    reportBuilder.append("<p id='").append(key.substring(0, key.indexOf('.'))).append("'></p>");
                reportBuilder.append("<table cellspacing='0' cellpadding='5'>");
                for (Object delta : patch.getDeltas()) {
                    Delta<String> del = (Delta<String>) delta;
                    builder.append("Master : ").append(del.getOriginal().toString().replaceAll("<", "&lt;").trim());
                    builder.append("Build  : ").append(del.getRevised().toString().replaceAll("<", "&lt;").trim());
                    builder.append("********");

                    reportBuilder.append("<tr class='master'><td>").append(del.getOriginal().getPosition() + 1).append("</td><td>").append(del.getOriginal().getLines().toString().replaceAll("<", "&lt;").trim()).append("</td></tr>");
                    reportBuilder.append("<tr class='child'><td>").append(del.getRevised().getPosition() + 1).append("</td><td>").append(del.getRevised().getLines().toString().replaceAll("<", "&lt;").trim()).append("</td></tr>");

                    reportBuilder.append("<tr><td colspan=2>&nbsp;</td></tr>");
                }
                reportBuilder.append("</table><br><br>");
                }
                else {
                    if(!report.contains(diffString.toString())){
                        report.add(diffString.toString());
                    }
                }
                map.put(key, diffString.toString());

                try {
                    new File(CONFIG.ROOT + File.separator + "diff").mkdirs();
                    new File(CONFIG.ROOT + File.separator + "diff" + File.separator + key).createNewFile();
                    FileWriter writer = new FileWriter(CONFIG.ROOT + File.separator + "diff" + File.separator + key);
                    writer.write(builder.toString());
                    writer.close();
                    //diffFiles.setProperty(key, masterList.getProperty(key));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }}
    }

    private void getKeyCount(StringBuilder reportBuilder,Object value){
        Set<String> ref = map.keySet();
        Iterator<String> it = ref.iterator();
        List<String> list = new ArrayList();

        while (it.hasNext()) {
            String o = it.next();
            if(map.get(o).equals(value)) {
                list.add(o);
            }
        }
        if(!list.isEmpty()){
            repeatReport(reportBuilder,list.get(0),list.size());
        }
    }
    private void repeatReport(StringBuilder reportBuilder,String url,Integer count){
        reportBuilder.append("$(\"#").append(url, 0, url.indexOf('.')).append("\").append(\"Repeated: ").append(count).append(" times\");");
    }
    /**
     * Maps the characters and its frequency of occurrence in the given content
     *
     * @param s
     * @return
     */
    private Map<Character, Integer> getFrequencyMap(String s) {
        Map<Character, Integer> frequencyMap = new TreeMap<>();
        for (int i = 0; i < s.length(); i++) {
            Character c = s.charAt(i);
            if (frequencyMap.containsKey(c)) {
                frequencyMap.put(c, frequencyMap.get(c) + 1);
            } else {
                frequencyMap.put(c, 0);
            }
        }
        return frequencyMap;
    }


    /**
     * Loads file content from the given filepath.
     *
     * @param filePath
     * @return
     */
    private String getFileContent(String filePath) {
        try {
            if (Files.exists(Paths.get(filePath))) {
                BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
                StringBuilder result = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
//                    line = line.replaceAll("[0-9]{13}", "0");
//                    line = line.replaceAll("[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}", "0");
                    result.append(line).append(System.lineSeparator());
                }
                br.close();
                return result.toString();
            } else {
                return "";
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Loads file content and returns a list of lines.
     *
     * @param filename
     * @return
     */
    private static List fileToLines(String filename) {
        List lines = new LinkedList();
        String line;
        try {
            if (new File(filename).exists()) {
                BufferedReader in = new BufferedReader(new FileReader(filename));
                while ((line = in.readLine()) != null) {
                    line = line.replaceAll("[0-9]{13}", "0");
                    line = line.replaceAll("[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}", "0");
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

}
