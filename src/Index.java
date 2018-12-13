import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author naveen-5141
 */

public class Index {

    private static HttpClient httpClient = HttpClientBuilder.create().setRedirectStrategy(new DefaultRedirectStrategy() {
        /** Redirectable methods. */
        private String[] REDIRECT_METHODS = new String[]{
                HttpGet.METHOD_NAME, HttpPost.METHOD_NAME, HttpHead.METHOD_NAME
        };

        @Override
        protected boolean isRedirectable(String method) {
            for (String m : REDIRECT_METHODS) {
                if (m.equalsIgnoreCase(method)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected URI createLocationURI(String location) throws ProtocolException {
            String uri = location.split(":" + CONFIG.PORT)[0];
            return super.createLocationURI(getEncodedURL(uri));
        }
    }).build();

    private static HttpClientContext httpContext;

    private static Map<String, CookieStore> userToCookieMap = new HashMap<String, CookieStore>();
    private static Integer count = 1;
    private static int retrycount = 1;
    private static String BUILD_TYPE = null;
    private static Configuration CONFIG = Configuration.loadConfiguration();
    private static StringBuilder diffurls = new StringBuilder();
    private static Logger logger = Configuration.getLogger(Index.class.getName());
    private static JSONObject urlObj;
    private static String fileName = "";


    /**
     * @param args
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        String currentDirectory = Paths.get(".").toAbsolutePath().normalize().toString();
        String os = System.getProperty("os.name");
        String fileExtension;
        if (os.contains("win") || os.contains("Win")) {
            fileExtension = ".bat";
        } else {
            fileExtension = ".sh";
        }

//        if(CONFIG.LOG_URL){
            String xmlContentToBeAdded = getFileContent(currentDirectory + File.separator + "WEB-INF" + File.separator + "web.xml");
            logger.log(Level.INFO, currentDirectory);
            String existingXMLContent = getFileContent(CONFIG.BUILD_HOME + File.separator + "webapps" + File.separator + "ROOT" + File.separator + "WEB-INF" + File.separator + "web.xml");
//            logger.log(Level.INFO, existingXMLContent);
        if(!existingXMLContent.contains("<filter-name>UrlCaptureFilter</filter-name>")) {
                Document doc = Jsoup.parse(existingXMLContent, "", Parser.xmlParser());
                Elements elements = doc.select("filter-mapping");
                for (Element element : elements) {
                    Elements elements1 = element.getAllElements();
                    for (Element element1 : elements1) {
                        if (element1.tagName().equalsIgnoreCase("url-pattern") && element1.text().equalsIgnoreCase("/j_security_check")) {
                            if (element1.previousElementSibling().text().equalsIgnoreCase("encodingFilter")) {
                                element1.parent().after(xmlContentToBeAdded);
                            }
                        }
                    }
                }
                doc.outputSettings(new Document.OutputSettings().prettyPrint(false));
                Files.write(Paths.get(CONFIG.BUILD_HOME + File.separator + "webapps" + File.separator + "ROOT" + File.separator + "WEB-INF" + File.separator + "web.xml"), doc.html().getBytes());
            }

            String content = getFileContent(CONFIG.BUILD_HOME + File.separator + "bin" + File.separator + "backUpData" + fileExtension);
            content = content.replace("commonsdputils.jar", "commonsdputils-automation.jar");
            Files.write(Paths.get(CONFIG.BUILD_HOME + File.separator + "bin" + File.separator + "backUpData-automation" + fileExtension), content.getBytes());

            if(!new File(CONFIG.BUILD_HOME + File.separator + "lib" + File.separator + "commonsdputils-automation.jar").exists()) {
                Files.copy(Paths.get(CONFIG.BUILD_HOME + File.separator + "lib" + File.separator + "commonsdputils.jar"), Paths.get(CONFIG.BUILD_HOME + File.separator + "lib" + File.separator + "commonsdputils-automation.jar"));
            }
            String cmd = "jar uvf " + CONFIG.BUILD_HOME + File.separator + "lib" + File.separator + "commonsdputils-automation.jar -C " + currentDirectory + "classes" + File.separator + "backup " + "com/";
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader jarreader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String opline;
            while ((opline = jarreader.readLine()) != null) {
                System.out.println(opline);
            }
            p.waitFor();


        if (CONFIG.EXECUTE_URL) {
            logger.log(Level.INFO, "Going to execute URL's");
            runAllUrl();
        }


        if (CONFIG.PERSIST_SQL_FILES) {
            System.out.println("Taking Backup...");

            logger.log(Level.INFO, os);

            new File(CONFIG.BUILD_HOME + File.separator + "bin" + File.separator + "backUpData" + fileExtension).setExecutable(true);
            ProcessBuilder pb = new ProcessBuilder(CONFIG.BUILD_HOME + File.separator + "bin" + File.separator + "backUpData" + fileExtension);
            pb.directory(new File(CONFIG.BUILD_HOME + File.separator + "bin"));
            Process pr = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                logger.log(Level.INFO, line);
            }
            pr.waitFor();
            moveSQLFiles();
        }


        if (CONFIG.COMPARE_RESPONSE || CONFIG.COMPARE_SQL) {
            DiffUtil rs = new DiffUtil();
            rs.init();
        }

    }

    /**
     * Moving sql files from backup dir to specified output dir
     */
    private static void moveSQLFiles() {
        File file = getLatestBackupDir(CONFIG.BUILD_HOME + File.separator + "backup");
        File[] files = file.listFiles();
        assert files != null;
        for (File f : files) {
            if (f.getName().endsWith("sql")) {
                try {
                    new File(CONFIG.OUTPUT_DIR + File.separator + "sql").mkdirs();
                    Files.move(f.toPath(), Paths.get(CONFIG.OUTPUT_DIR + File.separator + "sql" + File.separator + f.getName()), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }

    }

    /**
     * Execution of URLs
     *
     * @throws IOException
     * @throws URISyntaxException
     */
    private static void runAllUrl() throws IOException, URISyntaxException {
        FileReader fr = new FileReader(new File(CONFIG.URL_FILE));
        BufferedReader br = new BufferedReader(fr);
        String line;
        Long nanoTime = 0L;
        JSONArray urlList = new JSONArray();
        long lineCount = Files.lines(Paths.get(CONFIG.URL_FILE)).count();
        try (ProgressBar pb = new ProgressBar("Executing URL's", lineCount, ProgressBarStyle.ASCII)) {
            new File(CONFIG.OUTPUT_DIR + File.separator + "fileList.json").createNewFile();
            Files.write(Paths.get(CONFIG.OUTPUT_DIR + File.separator + "fileList.json"),"".getBytes(),StandardOpenOption.TRUNCATE_EXISTING);
            while ((line = br.readLine()) != null) {
                pb.step();
                retrycount = 0;
                try {
                    if (!line.isEmpty()) {
                        String loginName = line.split("###")[0];
                        String operation = line.split("###")[1];
                        Long portalId = null;
                        String url;
                        urlObj = new JSONObject();

                        if(BUILD_TYPE == null){
                            BUILD_TYPE = line.split("###").length > 3? "ESM": "NON_ESM";
                        }

                        if(BUILD_TYPE.equalsIgnoreCase("ESM")) {
                            try {
                                portalId = Long.valueOf(line.split("###")[2]);
                            } catch (Exception ignored) {
                            }
                            url = line.split("###")[3];
                        }
                        else {
                            url = line.split("###")[2];
                        }


                        /* remove state_id part from the url as it is not needed */
                        if (url.contains("STATE_ID/")) {
                            url = url.replaceAll("STATE_ID/(\\d+)/", "");
                        }

                        if (isURLInExclusionList(url)) {
                            count++;
                            continue;
                        }

                        if (CONFIG.WAIT_TIME > 0L) {
                            try {
                                TimeUnit.MILLISECONDS.sleep(CONFIG.WAIT_TIME);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        try {
                            switch (operation) {
                                case "GET":
                                    nanoTime = get(loginName, url, portalId);
                                    break;
                                case "POST":
                                    nanoTime = post(loginName, url, portalId);
                                    break;
                                case "PUT":
                                    nanoTime = put(loginName, url, portalId);
                                    break;
                                case "DELETE":
                                    nanoTime = delete(loginName, url, portalId);
                                    break;
                                case "MULTIPART":
                                    nanoTime = multipart(loginName, url, portalId);
                                    break;
                                case "JSONPOST":
                                    nanoTime = jsonpost(loginName, url, portalId);
                                    break;
                                case "JSONPUT":
                                    nanoTime = jsonput(loginName, url, portalId);
                                	break;
                            }
                        } catch (Exception e){
                            logger.log(Level.SEVERE, e.getMessage(), e);
                        }
                        urlObj.put("Index", (urlList.length()+1));
                        urlObj.put("File Name", fileName);
                        urlObj.put("Login Name", loginName);
                        urlObj.put("Operation", operation);
                        if(BUILD_TYPE.equalsIgnoreCase("ESM")){
                            urlObj.put("Portal Id", portalId);
                        }
                        urlObj.put("URL", url);
                        urlObj.put("Time", nanoTime);
                        urlList.put(urlObj);
                    }
                    //diffurls.append(line).append(System.lineSeparator());
                    count++;
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }

        Files.write(Paths.get(CONFIG.OUTPUT_DIR + File.separator + "fileList.json"), urlList.toString().getBytes());
    }
    
    private static Long multipart(String loginName, String url, Long portalId) throws IOException {
        String queryString = url.split("\\?").length > 1 ? url.split("\\?", 2)[1] : "";
        HttpPost postReq = new HttpPost(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT + url.split("\\?")[0]);
        postReq.setHeader("urlautomationcount", count.toString());
        queryString = handleCSRF(queryString);
        HttpEntity entity = getMultiPartEntity(queryString);
        httpContext = (HttpClientContext) getHttpContext(loginName, portalId);
        postReq.setEntity(entity);
        Long initTime = System.currentTimeMillis();
        HttpResponse res = httpClient.execute(postReq, httpContext);
        Long respTime = System.currentTimeMillis();
        Boolean isSuccess = writeResponse(res, loginName);
        if (!isSuccess) {
            retrycount++;
            userToCookieMap.remove(loginName);
            multipart(loginName, url, portalId);
        }
        postReq.releaseConnection();
        return (respTime-initTime);
    }

    private static HttpEntity getMultiPartEntity(String queryString) {

        MultipartEntityBuilder builder = MultipartEntityBuilder
                .create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.setBoundary("----WebKitFormBoundary" + UUID.randomUUID().toString());
        String[] params = queryString.split("&");
        for (String param : params) {
            if (param.contains("=")) {
                String key = param.split("=", 2)[0];
                String value = param.split("=", 2)[1];
                if (key.equalsIgnoreCase("serviceCatLogo")) {
                    builder.addPart(key, new FileBody(new File(CONFIG.BUILD_HOME + File.separator + "images" + File.separator + "systray_enabled.png"), ContentType.APPLICATION_OCTET_STREAM));
                } else {
                    try {
                        builder.addTextBody(key, URLDecoder.decode(value, "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return builder.build();
    }


    /**
     * Check if the url is to be skipped as configured in config.json.
     *
     * @param url
     * @return
     */
    private static Boolean isURLInExclusionList(String url) {
        Boolean toBeSkipped = false;
        JSONArray excludes = CONFIG.excludesList;
        for (int i = 0; i < excludes.length(); i++) {
            JSONObject excludeUrl = excludes.getJSONObject(i);
            if (excludeUrl.getString("type").equals("contains") && url.contains(excludeUrl.getString("string"))) {
                toBeSkipped = true;
            } else if (excludeUrl.getString("type").equals("startswith") && url.startsWith(excludeUrl.getString("string"))) {
                toBeSkipped = true;
            } else if (excludeUrl.getString("type").equals("regex")) {
                Pattern p = Pattern.compile(excludeUrl.getString("string"));
                Matcher m = p.matcher(url);
                if (m.find()) {
                    toBeSkipped = true;
                }
            }
        }
        return toBeSkipped;
    }

    /**
     * @param loginName
     * @param url
     * @throws IOException
     * @throws URISyntaxException
     */
    private static Long get(String loginName, String url, Long portalId) throws IOException, URISyntaxException {
        HttpGet getReq = new HttpGet(getEncodedURL(url));
        getReq.setHeader("urlautomationcount", count.toString());
        Long initTime = System.currentTimeMillis();
        HttpResponse res = httpClient.execute(getReq, getHttpContext(loginName, portalId));
        Long respTime = System.currentTimeMillis();
        Boolean isSuccess = writeResponse(res, loginName);
        if (!isSuccess) {
            retrycount++;
            userToCookieMap.remove(loginName);
            get(loginName, url, portalId);
        }
        getReq.releaseConnection();
        return (respTime-initTime);
    }

    /**
     * @param loginName
     * @param url
     * @throws IOException
     * @throws URISyntaxException
     */
    private static Long post(String loginName, String url, Long portalId) throws IOException {
        String queryString = url.split("\\?").length > 1 ? url.split("\\?", 2)[1] : "";
        HttpPost postReq = new HttpPost(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT + url.split("\\?")[0]);
        postReq.setHeader("Content-Type", "application/x-www-form-urlencoded");
        postReq.setHeader("urlautomationcount", count.toString());
        httpContext = (HttpClientContext) getHttpContext(loginName, portalId);
        queryString = handleCSRF(queryString);
        postReq.setEntity(new StringEntity(queryString, "UTF-8"));
        Long initTime = System.currentTimeMillis();
        HttpResponse res = httpClient.execute(postReq, httpContext);
        Long respTime = System.currentTimeMillis();
        Boolean isSuccess = writeResponse(res, loginName);
        if (!isSuccess) {
            retrycount++;
            userToCookieMap.remove(loginName);
            post(loginName, url, portalId);
        }
        postReq.releaseConnection();
        return (respTime-initTime);

    }

    /**
     * @param loginName
     * @param url
     * @throws IOException
     * @throws URISyntaxException
     */
    private static Long jsonpost(String loginName, String url, Long portalId) throws IOException, URISyntaxException {
        String queryString = url.split("\\?").length > 1 ? url.split("\\?", 2)[1] : "";
        String jsonParams = queryString.split("JSONPARAMETERS#",2)[1];
        queryString = queryString.split("JSONPARAMETERS#",2)[0];
        queryString = handleCSRF(queryString);
        HttpPost postReq = new HttpPost(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT  + url.split("\\?" )[0] + "?" + queryString);
        postReq.setHeader("Content-Type", "application/json");
        postReq.setHeader("urlautomationcount", count.toString());
        httpContext = (HttpClientContext) getHttpContext(loginName, portalId);
        postReq.setEntity(new StringEntity(jsonParams, ContentType.APPLICATION_JSON));
        Long initTime = System.currentTimeMillis();
        HttpResponse res = httpClient.execute(postReq, httpContext);
        Long respTime = System.currentTimeMillis();
        Boolean isSuccess = writeResponse(res, loginName);
        if (!isSuccess) {
            retrycount++;
            userToCookieMap.remove(loginName);
            jsonpost(loginName, url, portalId);
        }
        postReq.releaseConnection();
        return (respTime-initTime);

    }

    /**
     * @param loginName
     * @param url
     * @throws IOException
     * @throws URISyntaxException
     */
    private static Long put(String loginName, String url, Long portalId) throws IOException, URISyntaxException {
        String queryString = url.split("\\?").length > 1 ? url.split("\\?")[1] : "";
        HttpPut httpPut = new HttpPut(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT + url.split("\\?")[0]);
        httpPut.setHeader("Content-Type", "application/x-www-form-urlencoded");
        httpPut.setHeader("urlautomationcount", count.toString());
        httpContext = (HttpClientContext) getHttpContext(loginName, portalId);
        queryString = handleCSRF(queryString);
        httpPut.setEntity(new StringEntity(queryString));
        Long initTime = System.currentTimeMillis();
        HttpResponse res = httpClient.execute(httpPut, httpContext);
        Long respTime = System.currentTimeMillis();
        Boolean isSuccess = writeResponse(res, loginName);
        if (!isSuccess) {
            retrycount++;
            userToCookieMap.remove(loginName);
            put(loginName, url, portalId);
        }
        httpPut.releaseConnection();
 	    return (respTime-initTime);
    }

    /**
     * @param loginName
     * @param url
     * @throws IOException
     * @throws URISyntaxException
     */
    private static Long jsonput(String loginName, String url, Long portalId) throws IOException {
        String queryString = url.split("\\?").length > 1 ? url.split("\\?")[1] : "";
        String jsonParams = queryString.split("JSONPARAMETERS#",2)[1];
        queryString = queryString.split("JSONPARAMETERS#",2)[0];
        queryString = handleCSRF(queryString);
        HttpPut httpPut = new HttpPut(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT + url.split("\\?")[0] + "?" + queryString);
        httpPut.setHeader("Content-Type", "application/json");
        httpPut.setHeader("urlautomationcount", count.toString());
        httpContext = (HttpClientContext) getHttpContext(loginName, portalId);
        httpPut.setEntity(new StringEntity(jsonParams, ContentType.APPLICATION_JSON));
        Long initTime = System.currentTimeMillis();
        HttpResponse res = httpClient.execute(httpPut, httpContext);
        Long respTime = System.currentTimeMillis();
        Boolean isSuccess = writeResponse(res, loginName);
        if (!isSuccess) {
            retrycount++;
            userToCookieMap.remove(loginName);
            jsonput(loginName, url, portalId);
        }
        httpPut.releaseConnection();
        return (respTime-initTime);
    }

    /**
     * @param loginName
     * @param url
     * @throws IOException
     * @throws URISyntaxException
     */
    private static Long delete(String loginName, String url, Long portalId) throws IOException, URISyntaxException {
        HttpDelete httpDelete = new HttpDelete(getEncodedURL(url));
        httpDelete.setHeader("urlautomationcount", count.toString());
        Long initTime=0L,respTime=0L;
        try {
        	initTime = System.currentTimeMillis();
            HttpResponse res = httpClient.execute(httpDelete, getHttpContext(loginName, portalId));
            respTime = System.currentTimeMillis();
            Boolean isSuccess = writeResponse(res, loginName);
            if (!isSuccess) {
                retrycount++;
                userToCookieMap.remove(loginName);
                delete(loginName, url, portalId);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            httpDelete.releaseConnection();
        }
        return (respTime-initTime);
    }

    /**
     * Returns jsessionid and other cookies for the user specified.
     * Currently it is assumed loginname is equal to password need to change implementation if necessary.
     *
     * @param loginName
     * @return
     */
    private static CookieStore getCookieStore(String loginName) {
        if (userToCookieMap.containsKey(loginName)) {
            return userToCookieMap.get(loginName);
        } else {
            httpClient = HttpClientBuilder.create().setRedirectStrategy(new DefaultRedirectStrategy() {
                /** Redirectable methods. */
                private String[] REDIRECT_METHODS = new String[]{
                        HttpGet.METHOD_NAME, HttpPost.METHOD_NAME, HttpHead.METHOD_NAME
                };

                @Override
                protected boolean isRedirectable(String method) {
                    for (String m : REDIRECT_METHODS) {
                        if (m.equalsIgnoreCase(method)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                protected URI createLocationURI(String location) throws ProtocolException {
                    String uri = location.split(":" + CONFIG.PORT, 2)[1];
                    return super.createLocationURI(getEncodedURL(uri));
                }
            }).build();
            httpContext = new HttpClientContext();
            HttpPost request = new HttpPost(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT + "/j_security_check");
            List<NameValuePair> postParameters = new ArrayList<NameValuePair>();
            postParameters.add(new BasicNameValuePair("j_username", loginName));
            postParameters.add(new BasicNameValuePair("j_password", loginName));
            try {
                request.setEntity(new UrlEncodedFormEntity(postParameters, "UTF-8"));
                httpClient.execute(request, httpContext);
            } catch (ClientProtocolException e) {
                logger.log(Level.INFO, "First Time Login"); // Client Protocol Exception occurs when redirecting on first login
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
            request.releaseConnection();
            CookieStore cookieStore = httpContext.getCookieStore();
            userToCookieMap.put(loginName, cookieStore);
            return cookieStore;
        }
    }

    /**
     * Writes output to corresponding html/json files.
     *
     * @param res
     * @param loginName
     * @return
     * @throws IOException
     */
    private static Boolean writeResponse(HttpResponse res, String loginName) throws IOException {
        InputStream inp = res.getEntity().getContent();
        fileName = "url" + count;
        logger.log(Level.INFO, fileName + " " + res.getStatusLine().getStatusCode());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inp))) {
            CookieStore cookieStore = httpContext.getCookieStore();
            userToCookieMap.put(loginName, cookieStore);
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                result.append(line).append(System.lineSeparator());
            }

            if ((res.getStatusLine().getStatusCode() == 400 || result.toString().contains("<div id='loginDiv'") || result.toString().contains("class=\"login-section loginform\"") || result.toString().contains("Technician key in the request is invalid"))) { // TODO better implementation
                if(retrycount < 4) {
                    return false;
                }
            }
            String content = result.toString();

            if (res.getFirstHeader("Content-Type") != null && res.getFirstHeader("Content-Type").getValue().contains("application/json")) {
                fileName += ".json";
            } else if (res.getFirstHeader("Content-Type") != null && res.getFirstHeader("Content-Type").getValue().contains("text/xml")) {
                fileName += ".xml";
            } else if (!(content.contains("<html>") || content.contains("<body>") || content.contains("<head>") || content.contains("<div>") || content.contains("<td>"))) {
                fileName += ".txt";
            } else {
                fileName += ".html";
            }

            Files.write(Paths.get(CONFIG.OUTPUT_DIR + File.separator + fileName), content.getBytes());

        }
        return true;
    }

    /**
     * Replace the CSRF token value in the query string with value present in the sdpcsrfparam cookie header.
     *
     * @param queryString
     * @return
     */
    private static String handleCSRF(String queryString) {
        if (queryString.contains("sdpcsrfparam")) {
            String repl = queryString.substring(queryString.indexOf("sdpcsrfparam") + 13, queryString.indexOf("sdpcsrfparam") + 36 + 13);
            List<Cookie> cookies = httpContext.getCookieStore() != null ? httpContext.getCookieStore().getCookies() : null;
            if (cookies != null) {
                for (Cookie cookie : cookies) {
                    if (cookie.getName().equals("sdpcsrfcookie")) {
                        queryString = queryString.replace(repl, cookie.getValue());
                    }
                }
            }
        }
        return queryString;
    }

    /**
     * Returns httpContext with cookies loaded for specified user.
     *
     * @param loginName
     * @return
     */
    private static HttpContext getHttpContext(String loginName, Long portalId) {
        httpContext = new HttpClientContext();
        try {
            CookieStore cookieStore = getCookieStore(loginName);
            if (portalId != null) {
                BasicClientCookie cookie = new BasicClientCookie("PORTALID", portalId.toString());
                cookie.setPath("/");
                cookie.setDomain(CONFIG.HOST);
                cookieStore.addCookie(cookie);
            }
            httpContext.setCookieStore(cookieStore);


        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return httpContext;
    }

    /**
     * Encodes url params and returns fully qualified URL
     *
     * @param url
     * @return
     */
    private static String getEncodedURL(String url) {
        URIBuilder uri;
        try {
            uri = new URIBuilder(CONFIG.PROTOCOL + CONFIG.HOST + ":" + CONFIG.PORT + url.split("\\?")[0]);
            String queryString = url.split("\\?").length > 1 ? url.split("\\?", 2)[1] : "";
            List<NameValuePair> params = URLEncodedUtils.parse(queryString, Charset.forName("UTF-8"));
            for (NameValuePair param : params) {
                if (param.getValue() != null) {
                    uri.addParameter(param.getName(), param.getValue());
                }
            }
            return uri.toString();
        } catch (URISyntaxException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }


    /**
     * Returns the latest backup directory created
     *
     * @param dir
     * @return
     */
    private static File getLatestBackupDir(String dir) {
        File fl = new File(dir);
        File choice = null;
        File[] listFiles = fl.listFiles();
        if (listFiles != null && listFiles.length > 0) {
            File[] files = fl.listFiles(File::isDirectory);
            long lastMod = Long.MIN_VALUE;

            if (files != null) {
                for (File file : files) {
                    if (file.lastModified() > lastMod) {
                        choice = file;
                        lastMod = file.lastModified();
                    }
                }
            }
        }
        return choice;
    }

    /**
     * Loads file content from the given filepath.
     *
     * @param filePath
     * @return
     */
    private static String getFileContent(String filePath) {
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
            return "";
        }
    }

}
