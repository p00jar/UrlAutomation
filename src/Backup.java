//$Id$

import com.adventnet.mfw.ConsoleOut;
import com.adventnet.persistence.ConfigurationParser;
import com.adventnet.servicedesk.server.utils.SDDataManager;
import com.adventnet.servicedesk.tools.ConnectionUtil;
import com.manageengine.servicedesk.security.VulnerabilityUtil;
import com.zoho.framework.utils.crypto.CryptoUtil;
import com.zoho.framework.utils.crypto.EnDecryptImpl;
import net.lingala.zip4j.io.ZipOutputStream;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.swing.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.sql.*;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.Date;

/**
 * @author Murugesan K
 * This abstract class used fot getting backup related information
 */
public class Backup
{

    private Connection conn = null;
    private String separator = File.separator;
    private String backupDir = null;
    private String backupFileName = null;
    private BufferedWriter logout = null;
    private ArrayList tableNames = null;
    private ArrayList extendedTableListArray = new ArrayList();
    private String dbserver = null;
    //private String dbname = "mysql";//No I18N
    private String schedule_backdir = null;
    private	String database = null;
    private	String attachment = null;
    private boolean dbbackupstatus = true;
    private boolean dbhealthstatus = true;
    private static int attempt = 1;
    private String newline="__NEW_LINE__";//No I18N
    private String newlinechar="__#CRLF10#__";//No I18N
    private String crglinechar ="__#CRLF13#__";//No I18N
    private String dynamicTablesListSuffix = "DynamicTables.list";// No i18n

    private static ZipOutputStream[] zipout = new ZipOutputStream[100]; //Points to the .data file into which the data is zipped
    private File[] fileObjZipout= new File[100]; //File object of the above ZipOutputStream.
    private int ithzip = 0; //Holds the number of the .data file.
    private long backupFileSize = 0; //This holds the current size of the .data file
    private int filenamecount = 1; //Holds the part number of the file. i.e. backup_dbserver_builnumber_bkptype_timestamp_part_FILENAMECOUNT.data
    private static int maxfilesize = 1024 * 1024 * 1024; //default size of the split backup files
    private FileOutputStream fileList = null; //Points to the filelist.txt file that holds the name of all the backup files
    private boolean isNetworkAttachment = false; //This will be set true when the fileAttchments folder is in network path
    private int fileAttachmentsNumber; //Holds the number of fileAttachments.zip created.
    public String filenametoschedule = null; //holds all the backupfilenames (comma separated) and this is passed to scheduledbackuptask for db update.
    private static int psCount=0;
    public LinkedList skippedFilesList= new LinkedList();
    private boolean update = false;
    private static boolean skipFileExists = false;
    private static int maxsizeForSql = 1024*1024*1024; //max size for backup .sql file (1GB)

    private HashMap customFetchSizeMap;
    private static int defaultFetchSize = 1000;
    private HashMap<String, ArrayList<String>> scharTableCols = new HashMap<String, ArrayList<String>>();
    private HashMap<String,String> storeXMLvalues=new HashMap<String,String>();
    private String ecTag=null;
    private String cipher=null;
    private static HashMap confNameVsValue = new HashMap();
    private static HashMap confNameVsProps = new HashMap();

    //zip4j changes START
    private static ZipParameters toCompressFileParams = new ZipParameters();
    private String backupFilePassword = null;
    private String defaultBackupPassword = "SDP123!"; //No I18N
    private boolean isScheduledBackup = false;
    //zip4j changes END

    /**
     * @return void
     * This method will call all backup related method
     */
    public void process() throws Exception
    {
        try
        {
            //it360 chgs start here
			/* Dummy value set for prevent the schedule backup issue */
            String backupType= System.getProperty("sdp.backup.type","dummy");//No I18N

            //it360 chgs end here
            initBackup();
            writeDynamicTables("ServiceCatalog");//No i18N
            //it360 chgs start here
            if(!backupType.equals("--confiles"))
            {
                startBackup();
            }
            //it360 chgs end here
            endBackup();
            if(skipFileExists){
                logout.write("\n Backup Completed Partially. List of files skipped can be referred in skippedFiles.txt present in the .data files generated.");    // No I18N
            }else{
                logout.write("\nBackup Completed Successfully."); // No I18N
            }
        }
        catch(Exception e)
        {
            logout.write("\n\nBACKUP FAILED WITH ERRORS\n"); // No I18N
            printError(e);
            throw e;
        }
        finally
        {
//			deleteAllTempFiles();
            System.out.println("\n");//No I18N
            ConnectionUtil.shutDownDB();
            if( conn != null && !conn.isClosed() )
            {
                conn.close();
            }
            if( logout != null )
            {
                logout.flush();
                logout.close();
            }
        }
    }
    public Backup setUpgradeBackup(){
        this.update = true;
        return this;
    }

    public boolean getDBHealthStatus()
    {
        return dbhealthstatus;
    }

    public String getDefaultBackupPassword(){
        return defaultBackupPassword;
    }

    /**
     * Containing backup configuration information
     */
    public void setBackupConfiguration( java.util.HashMap hash ) throws Exception
    {
        maxfilesize = (Integer.parseInt(hash.get("BACKFILESIZE").toString())) * 1024 * 1024; //No I18N

        isScheduledBackup = true;

        getScheduledBackupConfiguredPasswordForBackup();

        if(backupFilePassword == null || "".equals(backupFilePassword)){
            throw new Exception("No password configured for scheduled backup.");
        }

        if( hash.size() > 1)
        {
            schedule_backdir = hash.get("BACKUPDIR").toString();//No I18N

            attachment = "false";//No I18N
            database = "true";//No I18N

            System.out.println(" HASH = " + hash );//No I18N
            System.out.println( "ATTACHMENT = " + hash.get("ATTACHMENT") );//No I18N
            System.out.println( "DATABASE = " + hash.get("DATABASE") );//No I18N
            if( "true".equals(hash.get("ATTACHMENT").toString().trim()) )
            {
                System.out.println("INSIDE ATTACHMENT");//No I18N
                attachment = "true";//No I18N
            }
            if( "false".equals(hash.get("DATABASE").toString().trim()) )
            {
                System.out.println("INSIDE DATABASE");//No I18N
                database = "false";//No I18N
            }
        }

    }

    /**
     * This method will return the current build number of SDP
     */
    public String getBuildNumber()
    {
        return SDDataManager.getInstance().getBuildNumber ();
    }

    /**
     *@return return all tables in the specified database
     * It will return all table names
     */
    private ArrayList getAllTables () throws Exception
    {
        ArrayList list = new ArrayList();
        // SD - 46409 dbAdapter.getTables retrieves system tables in SQL 2012 hence getTbLstFrTDetails has been defined.
        List tmp = ConnectionUtil.getTbLstFrTDetails();
        if( tmp != null )
        {
            int len = tmp.size();
            for( int i=0; i<len; i++ )
            {
                list.add(tmp.get(i));
            }
        }
        return list;
    }

    /**
     * Init method - will be called before taking backup
     */
    protected void initBackup () throws Exception
    {
        try
        {
            System.out.println( "Starting...." );//No I18N
            //it360 chgs start here
            //conn = getConnection();
            //it360 chgs end here
            File file = new File( "SDPbackup.log" );//No I18N
            logout = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(file),"UTF8") );//No I18N



            if( System.getProperty("sdp.backup.type") != null )
            {
                //Need to invoke this method when the backup is triggered from command prompt
                SDDataManager.setStandAlone(true);
            }
            //SD-62389 Error while trying to connect to Mysql bundled with application while taking manual backup over 9200 has been fixed.   
            System.setProperty("server.home",SDDataManager.getInstance().getRootDir()); //No I18N

            String build_number = getBuildNumber();
            //it360 chgs start here
            String backupType= System.getProperty("sdp.backup.type","dummy");//No I18N
            dbserver = ConnectionUtil.getActiveDBName();

            if("postgres".equals(dbserver))
            {
                storeXMLvalues=getProductConfig();
                ecTag=storeXMLvalues.get("ecTag");
                cipher=storeXMLvalues.get("cipher");
            }

            initializeZipFileParams();

            backupFileName = getBackupFileName(build_number);
            backupDir = getBackupDir();
            if( !new File(backupDir).isDirectory() )
            {
                new File(backupDir).mkdirs();
            }
            if(!backupType.equals("--confiles"))
            {
                conn = getConnection();

                tableNames = getAllTables();

                System.out.println("*************************************************************");//No I18N
                System.out.println("Number of tables found : " + tableNames.size());//No I18N
                System.out.println("-------------------------------------------------------------");//No I18N
                System.out.println("Build number           : " + build_number);//No I18N
                System.out.println("-------------------------------------------------------------");//No I18N

                createBuildInfoXml( build_number );

                backupFileName = getBackupFileName(build_number);

                logout.write("\nServiceDesk Plus [" + build_number + "] backup log");//No I18N
                logout.write("\n*************************************************************");//No I18N
            }//it360 chgs end here
        }
        catch(Exception e)
        {
            logout.write("\nException while init Backup"); //No I18N
            e.printStackTrace();
            throw e;
        }
    }

    //SD 67772 fix - Option to set fetch size for individual tables
    private void initializeCustomFetchSize(String customFetchSizeInput, HashMap pkdetails) throws Exception{
        customFetchSizeMap = new HashMap();
        logout.write("\n"); //No I18N
        if(customFetchSizeInput==null || "".equals(customFetchSizeInput)){
            logout.write("\nCustom fetch size not provided. Continuing with default fetch size = " + defaultFetchSize + " for all tables."); //No I18N
        }
        else {
            try {
                String[] customSizeList = customFetchSizeInput.split(",");
                for (int i = 0; i < customSizeList.length; i++) {
                    String[] val = customSizeList[i].split("=");
                    String tableName = val[0].trim();
                    try {
                        int fetchSizeForTable = Integer.parseInt(val[1]);
                        if (pkdetails.get(tableName.toLowerCase()) == null) {
                            logout.write("\nCustom fetch size cannot be applied to " + tableName + "."); //No I18N
                        } else if (fetchSizeForTable > 0 && fetchSizeForTable <= defaultFetchSize) {
                            customFetchSizeMap.put(tableName, fetchSizeForTable);
                        }
                    }
                    catch(NumberFormatException nfe){
                        logout.write("\nIncorrect fetch size supplied for " + tableName + ". Not setting custom fetch size."); //No I18N
                    }
                }
                logout.write("\nCustom fetch size map : " + customFetchSizeMap + "\n"); //No I18N
            } catch (Exception e) {
                logout.write("\nException while trying to set custom fetch size. Will continue with default fetch size = " + defaultFetchSize + " for all tables."); //No I18N
            }
        }
    }

    /**
     * This methos invoke the backup process
     */
    protected void startBackup () throws Exception
    {
        logout.write(" Backup process started....\n ");//No I18N
        //String backupDir = System.getProperty("sdp.backup.home");//No I18N
        //it360 chgs start here
        String backupType = System.getProperty("sdp.backup.type","dummy");//No I18N
        //it360 chgs end here
        if( System.getProperty("sdp.backup.ignoretables") != null )
        {
            String ignoreTables[] = System.getProperty("sdp.backup.ignoretables").split(",");//No I18N
            int size = ignoreTables.length;
            logout.write("\nIgnored tables list is : \n\n"); // No I18N
            System.out.println("Ignored Table List");//No I18N
            System.out.println("-------------------------------------------------------------");//No I18N
            for( int i=0; i<size; i++ )
            {
                System.out.println(ignoreTables[i]);
                logout.write(ignoreTables[i] + "\n"); // No I18N
                tableNames.remove(ignoreTables[i].trim()); // For MSSQL
                tableNames.remove(ignoreTables[i].toLowerCase().trim()); // For Mysql
            }
            System.out.println("-------------------------------------------------------------");//No I18N
            logout.write("\n\n"); // No I18N
        }
        //21167- Unable to ignore unwanted tables in scheduled backup

        else
        {
            java.sql.Statement stmt = null;
            try
            {
                stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery("select PARAMVALUE from GlobalConfig where CATEGORY='SCHEDULE_BACKUP_IGNORE_TABLES'");//No I18N
                if(rs.next())
                {
                    String ignoreTables[]=rs.getString("PARAMVALUE").split(","); //No I18N
                    int size = ignoreTables.length;
                    System.out.println("Ignored Table List"); //No I18N
                    logout.write("Ignore Table List\n"); // No I18N
                    System.out.println("-------------------------------------------------------------"); //No I18N
                    logout.write("\nIgnore tables list is : \n\n "); // No I18N
                    for( int i=0; i<size; i++ )
                    {
                        System.out.println(ignoreTables[i]+"\n");
                        logout.write(ignoreTables[i]);
                        tableNames.remove(ignoreTables[i].trim()); // For MSSQL
                        tableNames.remove(ignoreTables[i].toLowerCase().trim()); // For Mysql
                    }
                    logout.write("\n\n"); // No I18N
                    System.out.println("-------------------------------------------------------------");//No I18N
                }

                rs.close();
            }
            catch(Exception e)
            {
                logout.write("\nError while getting the ignore tables list"); // No I18N
                e.printStackTrace();
                throw e;

            }
            finally
            {
                if( stmt != null )
                {
                    stmt.close();
                }
            }


        }

        if( backupType != null && backupType.indexOf("--trim") >= 0 )
        {
            System.out.println( "Going to take minimal backup without file attachments" );//No I18N
            logout.write( "\nGoing to take minimal backup without file attachments" );//No I18N
        }
        else if( attachment != null && database != null && database.equals("true") && attachment.equals("true") )
        {
            System.out.println( "Going to take full backup with file attachments" );//No I18N
            logout.write( "\nGoing to full backup with file attachments" );//No I18N
        }
        else if( attachment != null && attachment.equals("true") )
        {
            dbbackupstatus = false;
            System.out.println( "Going to backup the file attachments" );//No I18N
            logout.write( "\nGoing to backup the file attachments" );//No I18N
        }
        else if( database != null && database.equals("true") )
        {
            System.out.println( "Going to backup the database" );//No I18N
            logout.write( "\nGoing to backup the file database" );//No I18N
        }
        else
        {
            System.out.println( "Going to take full backup with file attachments" );//No I18N
            logout.write( "\nGoing to full backup with file attachments" );//No I18N
        }
        System.out.println("-------------------------------------------------------------");//No I18N

        if( dbbackupstatus )
        {
            HashMap pkdetails = getPkDetails();

            //SD 67772 fix - Option to set fetch size for individual tables
            String tableRowFetchSizeInput = null;
            if(System.getProperty("sdp.backup.fetchsize") != null){
                tableRowFetchSizeInput = System.getProperty("sdp.backup.fetchsize");
            }
            else{
                java.sql.Statement stmt = null;
                ResultSet rs = null;
                try
                {
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery("select PARAMVALUE from GlobalConfig where CATEGORY='SCHEDULE_BACKUP_FETCHSIZE'"); //No I18N
                    if(rs.next()){
                        tableRowFetchSizeInput = rs.getString("PARAMVALUE");
                    }
                }
                catch(Exception e)
                {
                    logout.write("\nException while trying to read fetch size from GlobalConfig."); //No I18N
                    e.printStackTrace();
                    throw e;
                }
                finally
                {
                    if(rs!=null){
                        rs.close();
                    }
                    if( stmt != null )
                    {
                        stmt.close();
                    }
                }
            }
            initializeCustomFetchSize(tableRowFetchSizeInput, pkdetails);

            System.out.println("*************************************************************");//No I18N
            System.out.println("");//No I18N
            System.out.println ("\n\nPlease wait ! backup in Progress...................");//No I18N
            System.out.println ("0-----------------------50------------------------100(%)");//No I18N
            int len = tableNames.size();
            int divider = len / 50;
            logout.write("\n\nBackup of database started. Querying the database... \n\n"); // No I18N
            scharTableCols=getScharTableCols();
            for( int i=0; i<len; i++ )
            {
                if(i%divider == 0)
                {
                    System.out.print("=");//No I18N
                }
                try
                {
                    dumpTable( tableNames.get(i).toString(), (String)pkdetails.get(tableNames.get(i).toString().toLowerCase()) );
                }
                catch( Exception e )
                {
                    e.printStackTrace();
                    if( e.getMessage() != null && e.getMessage().indexOf("servicedesk.table_") >= 0 )
                    {
                        continue;
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }
        System.out.println ("");
    }

    private void writeDynamicTables(String module) throws Exception
    {
        java.sql.Statement stmt = null;
        BufferedWriter bw = null;
        ResultSet rs = null;
        try
        {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select TABLENAME from DynamicTables where MODULE = '" + module + "'");//No I18N
            bw = new BufferedWriter(new FileWriter(new File(backupDir + separator + module+ dynamicTablesListSuffix)));
            while(rs.next())
            {
                String tName = rs.getString(1);
                if(tName != null)
                {
                    bw.write(tName);
                    bw.newLine();
                }
            }
            bw.flush();
            bw.close();
            rs.close();
            stmt.close();
        }
        catch(Exception e)
        {
            try
            {
                logout.write("\nFailed to write the list of dynamic tables for the module "+ module + ":" + e.getMessage());//No I18N
            }
            catch(Exception ee)
            {
                ee.printStackTrace();
            }
        }
        finally
        {
            if(stmt != null)
            {
                stmt.close();
            }
            if(bw != null)
            {
                bw.close();
            }
            if(rs != null)
            {
                rs.close();
            }
        }
    }

    private void deleteDynmicTablesFile(String module)
    {
        try
        {
            new File(backupDir + separator + module + dynamicTablesListSuffix).delete();
        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }


    private HashMap getPkDetails() throws Exception
    {
        HashMap pkdetails = new LinkedHashMap();
        java.sql.Statement stmt = null;
        ResultSet rs = null;

        try
        {
            stmt = conn.createStatement();

            rs = stmt.executeQuery("select max(td.TABLE_NAME) as \"Table\", max(cd.COLUMN_NAME) as \"PKColumn\" from PKDefinition pkd LEFT JOIN ColumnDetails cd ON pkd.PK_COLUMN_ID = cd.COLUMN_ID LEFT JOIN TableDetails td ON cd.TABLE_ID = td.TABLE_ID where DATA_TYPE IN ('BIGINT','INTEGER') group by cd.TABLE_ID HAVING COUNT(PK_COLUMN_ID) = 1");//No I18N

            while( rs.next() )
            {
                pkdetails.put(rs.getString(1).toLowerCase(), rs.getString(2));
            }

        }
        catch(Exception e)
        {
        }
        finally
        {
            if(rs!= null)
            {
                rs.close();
            }
            if( stmt != null )
            {
                stmt.close();
            }
        }
        return pkdetails;
    }

    /**
     * This method will be called after getting backup
     */
    protected void endBackup () throws Exception
    {
        logout.write( "\nCompressing backup file\n" );//No I18N
        logout.flush();
        String rootdir = SDDataManager.getInstance().getRootDir();//No I18N

        Properties properties = new Properties();

        String attachmentPath = null;
        java.sql.Statement stmt = null;
        ResultSet rs = null;
        fileList = new FileOutputStream(new File(backupDir +separator + "filelist.txt")); //No I18N

        checkBackupFileSize();
        //it360 chgs start here
        String backupType = System.getProperty("sdp.backup.type","dummy");//No I18N
        //it360 chgs end here
        //it360 chgs start here
        if(!backupType.equals("--confiles"))
        {
            try
            {
                stmt = conn.createStatement();
                //Getting attachment path from GlobalConfig table
                rs = stmt.executeQuery("select PARAMVALUE from GlobalConfig where CATEGORY='FileAttachment' AND PARAMETER='Attachment_Path'");//No I18N
                if (rs.next())
                {
                    attachmentPath = rs.getString("PARAMVALUE");
                    //If file attachment path is not default then create FileAttachmentPath.properties and set attachment path to AttachmentPath key.
                    if (attachmentPath != null && "FileAttachments".equals(attachmentPath))
                    {
                        attachmentPath = rootdir;
                    }
                    else if (attachmentPath != null && !attachmentPath.equals(rootdir))
                    {
                        File isAttachmentExist = new File(attachmentPath + separator + "fileAttachments");
                        if (isAttachmentExist.exists())
                        {
                            isNetworkAttachment = true;
                            //Properties properties = new Properties();
                            properties.setProperty("AttachmentPath", attachmentPath);
                            //properties.store(new FileOutputStream(rootdir + separator + "FileAttachmentPath.properties"), null);
                        }
                    }
                }
            }
            catch(Exception e)
            {
                logout.write("\nError occured while taking the file attachments path from database."); // No I18N
                e.printStackTrace();
            }
            finally
            {
                if( stmt != null )
                {
                    stmt.close();
                }
                rs.close();
            }
        }

        //ZipOutputStream zipout = new ZipOutputStream (new FileOutputStream (backupDir +separator+ backupFileName));

        /** attachement files excluded  for taking minimal backup
         * How to invoke minimal backup enable :-
         * ]$ sh backUpData.sh --minimal - [ for linux users ]
         * C:>AdventNet\ME\ServiceDesk\bin backUpData.bat --minimal - [ for windows users ]
         * @author Murugesan K
         */


                /*
                 * Issue due to changes of IT360 in Branch. Any changes of IT360 should be above this code.
                 * SD-38856,Includes File attcahment in scheduled backup, rather it is not selected
                 */
        if("false".equals(attachment) && attachment!=null)
        {
            backupType="--trimmed";
        }

        if( ( attachment != null && attachment.equals("true") ) || (backupType != null && backupType.indexOf("--trim") == -1) )
        {
            try
            {
                new File(attachmentPath + separator + "fileAttachments" + separator + "thumbs.db").delete();//No I18N
                new File(rootdir + separator + "custom" + separator + "thumbs.db").delete();//No I18N
                new File(rootdir + separator + "inlineimages" + separator + "thumbs.db").delete();//No I18N
                new File(rootdir + separator + "fileAttachments" + separator + "Thumbs.db").delete();//No I18N
                new File(rootdir + separator + "custom" + separator + "Thumbs.db").delete();//No I18N
                new File(rootdir + separator + "inlineimages" + separator + "Thumbs.db").delete();//No I18N
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }
            //Checking whether network path is configured or not, if it so then fileAttachments folder is zipped and added to backup data.
            if(isNetworkAttachment){
                //sd-56813 : error when fileattachment in network path has not files.
                if(hasFile(attachmentPath + separator + "fileAttachments")){
                    filenamecount = 1;
                    //ZipOutputStream fileAttachmentZip = new ZipOutputStream (new FileOutputStream (rootdir +separator+ "FileAttachments.zip"));
                    String attachmentZip = rootdir + separator + "FileAttachments1.zip"; //No I18N
                    ithzip= initZipOut(attachmentZip);
                    zipDirectory(attachmentPath + separator + "fileAttachments");//No I18N
                    //fileAttachmentZip.flush();
                    //fileAttachmentZip.close();
                    zipout[ithzip].flush();
                    zipout[ithzip].finish();
                    zipout[ithzip].close();
                }
                else{
                    logout.write("\nNo File in FileAttachments. So not including fileAttachments in backup."); //No I18N
                }
                //sd-56813 ends
                //ithzip= initZipOut(attachmentZip);
                isNetworkAttachment = false;
                fileAttachmentsNumber=filenamecount;
            }

				/*if (ff.exists())
				{
                                    ithzip=0;

                                    fAnum=filenamecount;
                                    String fAnumStr;
                                    fAnumStr=String.valueOf(fAnum);
                                    properties.setProperty("FileAttachmentsNumber",fAnumStr );
                                    properties.store(new FileOutputStream(rootdir + separator + "FileAttachmentPath.properties"), null); //No I18N
                                       filenamecount=1;
                                       ithzip=initZipOut(backupDir + separator +  backupFileName.substring(0, backupFileName.lastIndexOf("_part_"))+ "_part_" + filenamecount + ".data");
					//addToZip( rootdir + separator + "FileAttachments.zip"); //No I18N
                                       for(int i=1;i<=fAnum;i++){
                                           addToZip(rootdir + separator + "FileAttachments" + i + ".zip"); //No I18N
				}

					addToZip(rootdir + separator + "FileAttachmentPath.properties"); // No I18N
				}
			} else {
                            ithzip=0;

                            filenamecount=1;
                                ithzip=initZipOut(backupDir + separator  + backupFileName.substring(0, backupFileName.lastIndexOf("_part_"))+"_part_"+filenamecount + ".data");
				//If network path is not configured then, fileAttachments folder in default path is added to backup data
				zipDirectory (attachmentPath + separator + "fileAttachments");//No I18N
			}

			zipDirectory (rootdir + separator + "custom");//No I18N
			zipDirectory (rootdir + separator + "inlineimages");//No I18N*/

            filenamecount = 1;
            ithzip=0;
            ithzip=initZipOut(backupDir + separator +  backupFileName.substring(0, backupFileName.lastIndexOf("_part_"))+ "_part_" + filenamecount + ".data");//No I18N
            String dbserver = ConnectionUtil.getActiveDBName();
            if(!dbserver.equals("postgres"))
            {
                if(System.getProperty("os.name").indexOf("Windows")!=-1)
                {
                    zipConfFiles (rootdir + separator + "bin" + separator + "startDB.bat");//No I18N
                }
                if("Linux".equals(System.getProperty("os.name")))
                {
                    zipConfFiles (rootdir + separator + "bin" + separator + "startDB.sh");//No I18N
                }
                zipConfFiles (SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"wrapper.conf");//No I18N
            }
            if(System.getProperty("os.name").indexOf("Windows")!=-1)
            {
                zipConfFiles (rootdir + separator + "bin" + separator + "run.bat");//No I18N
            }
            if("Linux".equals(System.getProperty("os.name")))
            {
                zipConfFiles (rootdir + separator + "bin" + separator + "run.sh");//No I18N
            }
            //External Action Plugin related script files included in backup
            zipDirectory (rootdir + separator + "integration");// No I18n
            String fosInputsConf = rootdir + separator + "fos" + separator + "fosInputs.conf";//No I18N
            if(new File(fosInputsConf).exists())
            {
                zipConfFiles(fosInputsConf);
            }

            zipConfFiles (SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"server.xml");//No I18N
            //SD-69271 Need to include web.xml and logging.properties file in backup.
            zipConfFiles (rootdir + separator + "conf" + separator + "web.xml");//No I18N
            zipConfFiles (rootdir + separator + "conf" + separator + "logging.properties");//No I18N
            zipConfFiles (rootdir + separator + "conf" + separator + "TrayIconInfo.xml");//No I18N
            //zipConfFiles (rootdir + separator + "server" + separator + "default" + separator + "conf" + separator + "sample-bindings.xml");//No I18N

            zipConfFiles (rootdir + separator + "applications" + separator + "extracted" + separator + "MSPDesk.eear" + separator + "axis.ear" + separator + "axis2.war" + separator + "WEB-INF" + separator + "conf" + separator + "axis2.xml");//No I18N       
            String keyFileName=getKeystoreFileName();
            if(keyFileName!=null){
                File f= new File(SDDataManager.getInstance().getRootDir()+"conf"+File.separator+keyFileName);
                if(f.exists()){
                    zipConfFiles (SDDataManager.getInstance().getRootDir()+"conf"+File.separator+keyFileName);//No I18N 
                }
            }
            zipDirectory (rootdir + separator + "app_relationships");// No I18n


        }

        try{

            if((backupType.indexOf("--trim")!=-1))
            {
                isNetworkAttachment = false;

                ithzip=initZipOut(backupDir + separator  + backupFileName.substring(0, backupFileName.lastIndexOf("_part_"))+"_part_"+filenamecount + ".data");//No I18N
            }



            // archive folder will be included in the trimmed backup itself...
            try
            {
                new File(rootdir + separator + "archive" + separator + "thumbs.db").delete();//No I18N
                new File(rootdir + separator + "archive" + separator + "Thumbs.db").delete();//No I18N
            }catch(Exception ex){
                ex.printStackTrace();
            }
            //These files will be included in trimmed backup also	
            zipConfFiles (SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"customer-config.xml");//No I18N
            zipConfFiles (rootdir + separator + "zreports" + separator + "uploadtool" + separator + "conf" + separator + "common_params.conf");//No I18N                        
            zipConfFiles (rootdir + separator + "zreports" + separator + "reference"+separator+"ZRMEModulesCopy.xml");// No I18n
            zipConfFiles (rootdir + separator + "zreports" + separator + "sdp"+separator+"DynamicModuleList.xml");// No I18n
            zipConfFiles (rootdir + separator + "zreports" + separator + "sdp"+separator+"IgnoredZRSRModulesList.conf");// No I18n
            zipConfFiles (rootdir + separator + "zreports" + separator + "sync"+separator+"conf"+separator+"ZRMEModules.xml");// No I18n
            zipConfFiles (rootdir + separator + "zreports" + separator + "uploadtool" + separator + "conf" + separator + "database_sql_queries.xml");//No I18N                        
            zipConfFiles (rootdir + separator + "zreports" + separator + "defaultreports" + separator + "reportslist.conf");//No I18N                        
            File processedZRModules=new File(rootdir + separator + "zreports" + separator +"newmodules"+separator+"processed");
            File processedZRMigFiles=new File(rootdir + separator + "zreports" + separator +"migration"+separator+"processed");
            if(processedZRModules.exists())
            {
                for(String processedZRModule:processedZRModules.list())
                {
                    zipConfFiles(processedZRModules.getAbsolutePath()+separator+processedZRModule);
                }
            }
            if(processedZRMigFiles.exists())
            {
                for(String processedZRMigFile:processedZRMigFiles.list())
                {
                    zipConfFiles(processedZRMigFiles.getAbsolutePath()+separator+processedZRMigFile);
                }
            }

            //zipDirectory (rootdir + separator + "archive");//No I18N
            //it360 chgs start here
            //addToZip( zipout, backupDir + separator + "backup_info.xml" );//No I18N
            //it360 chgs end here
            String license_file = rootdir + separator + "lib" + separator + "AdventNetLicense.xml";//No I18N
            if( new File(license_file).exists() )
            {
                addToZip( rootdir + separator + "lib" + separator + "AdventNetLicense.xml" );//No I18N
            }
            //it360 chgs start here
            if(!backupType.equals("--confiles"))
            {
                addToZip( backupDir + separator + "backup_info.xml" );//No I18N

                if( dbbackupstatus )
                {
                    int len = tableNames.size();

                    for( int i=0; i<len; i++ )
                    {
                        addToZip( backupDir + separator + tableNames.get(i).toString().toLowerCase() + ".sql");//No I18N
                    }

                    int len1 = extendedTableListArray.size();
                    for( int j=0; j<len1; j++ )
                    {
                        addToZip( backupDir + separator + extendedTableListArray.get(j).toString().toLowerCase() + ".sql");//No I18N
                    }

                    addToZip( backupDir + separator + "ServiceCatalog" + dynamicTablesListSuffix);//No I18N
                }
            }//it360 chgs end here
            String archiveDynamicFile = SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"SDPArchive_Dynamic.xml";//No I18N

            // backing up nio_port.json file if exists

            String nioPortFile = rootdir + separator + "conf"+ separator + "nio_port.json";//No I18N
            if(new File(nioPortFile).exists())
            {
                File portFile = new File(rootdir + separator + "conf","nio_port.json");
                //SD-69640 Problem occurs while including nio_port.json file in backup data.
                int portFileInx = portFile.getPath().indexOf("conf");
                setDirectoryForFileInZipFile("conf" + separator + "nio_port.json"); //No I18N
                zipout[ithzip].putNextEntry (portFile, toCompressFileParams);

                FileInputStream fileReader = new FileInputStream(portFile);
                try
                {
                    byte[]data = new byte[1024];
                    for (int j = 0;; j++)
                    {
                        int length = fileReader.read (data);
                        if (length < 0)
                        {
                            break;
                        }
                        zipout[ithzip].write (data, 0, length);
                        zipout[ithzip].flush ();
                    }
                    zipout[ithzip].closeEntry();
                }
                catch(Exception e)
                {
                    logout.write("\nError when adding nioPortFile to zip"); // No I18N
                    throw e;
                }
                finally
                {
                    fileReader.close();
                    setDirectoryForFileInZipFile("");
                }
            }


            if(new File(archiveDynamicFile).exists())
            {
                File archivefile = new File(SDDataManager.getInstance().getRootDir()+"conf","SDPArchive_Dynamic.xml");
                int archivefileInx = archivefile.getPath().indexOf("conf");
                setDirectoryForFileInZipFile("conf" + separator + "SDPArchive_Dynamic.xml"); //No I18N
                zipout[ithzip].putNextEntry (archivefile, toCompressFileParams);

                FileInputStream fileReader = new FileInputStream(archivefile);
                try
                {
                    byte[]data = new byte[1024];
                    for (int j = 0;; j++)
                    {
                        int length = fileReader.read (data);
                        if (length < 0)
                        {
                            break;
                        }
                        zipout[ithzip].write (data, 0, length);
                        zipout[ithzip].flush ();
                    }
                    zipout[ithzip].closeEntry();
                }
                catch(Exception e)
                {
                    logout.write("\nError when adding archiveDynamicFile to zip"); // No I18N
                    throw e;
                }
                finally
                {
                    fileReader.close();
                    setDirectoryForFileInZipFile("");
                }
            }
            String postgresqlConf = rootdir + separator + "pgsql" + separator + "data" + separator + "postgres_ext.conf";//No I18N

            if(new File(postgresqlConf).exists())
            {
                File postgresqlFile = new File(rootdir + separator + "pgsql" + separator + "data","postgres_ext.conf");
                int postgresfileInx = postgresqlFile.getPath().indexOf("pgsql");
                setDirectoryForFileInZipFile("pgsql" + separator + "data" + separator + "postgres_ext.conf"); //No I18N
                zipout[ithzip].putNextEntry (postgresqlFile, toCompressFileParams);

                FileInputStream fileReader = new FileInputStream(postgresqlFile);
                try
                {
                    byte[]data = new byte[1024];
                    for (int j = 0;; j++)
                    {
                        int length = fileReader.read (data);
                        if (length < 0)
                        {
                            break;
                        }
                        zipout[ithzip].write (data, 0, length);
                        zipout[ithzip].flush ();
                    }
                    zipout[ithzip].closeEntry();
                }
                catch(Exception e)
                {
                    logout.write("\nError while adding prostgres_ext.conf file to zip"); // No I18N
                    throw e;
                }
                finally
                {
                    fileReader.close();
                    setDirectoryForFileInZipFile("");
                }
            }
            String snmpConfigurationsXML = SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"SnmpConfigurations.xml";//No I18N
            if( new File(snmpConfigurationsXML).exists() )
            {
                addToZip( snmpConfigurationsXML );//No I18N
            }
            zipDirectory (rootdir + separator + "archive");//No I18N

            //Code to backup DID starts
            if(System.getProperty("os.name").indexOf("Windows")!=-1){ //DID value is available only in Windows
                try{
                    zipDirectory (rootdir + separator + "blog");//No I18N
                }
                catch(Exception e){
                    System.out.println("Error while backup blog folder"+e);
                }
            }
            //Code to backup DID ends

            if( ( attachment != null && attachment.equals("true") ) || (backupType != null && backupType.indexOf("--trim") == -1) )
            {
                zipDirectory (rootdir + separator + "custom");//No I18N
                zipDirectory (rootdir + separator + "inlineimages");//No I18N
                zipDirectory (rootdir + separator + "LuceneIndex");//No I18N
                zipDirectory (rootdir + separator + "webremoterecordedfiles");//No I18N
                if (new File(rootdir + separator + "FileAttachments1.zip").exists())
                {


                    filenamecount++;
                    String fileAttachmentsNumStr;
                    fileAttachmentsNumStr=String.valueOf(fileAttachmentsNumber);
                    properties.setProperty("FileAttachmentsNumber",fileAttachmentsNumStr );
                    properties.store(new FileOutputStream(rootdir + separator + "FileAttachmentPath.properties"), null); //No I18N

                    ithzip=initZipOut(backupDir + separator +  backupFileName.substring(0, backupFileName.lastIndexOf("_part_"))+ "_part_" + filenamecount + ".data");//No I18N
                    //addToZip( rootdir + separator + "FileAttachments.zip"); //No I18N
                    for(int i=1;i<=fileAttachmentsNumber;i++){
                        addToZip(rootdir + separator + "FileAttachments" + i + ".zip"); //No I18N
                    }

                    addToZip(rootdir + separator + "FileAttachmentPath.properties"); // No I18N
                }
                else {

                    if(new File(rootdir + separator + "fileAttachments").isDirectory() && hasFile(rootdir + separator + "fileAttachments")) //No I18N
                    {
                        filenamecount++;
                        ithzip=initZipOut(backupDir + separator  + backupFileName.substring(0, backupFileName.lastIndexOf("_part_"))+"_part_"+filenamecount + ".data");//No I18N
                        //If network path is not configured then, fileAttachments folder in default path is added to backup data
                        zipDirectory (attachmentPath + separator + "fileAttachments");//No I18N
                    }
                }
                //System.out.println(skippedFilesList.size());
                if(skippedFilesList.size() != 0){
                    File skippedFiles = new File(backupDir + File.separator + "skippedFiles.txt");
                    BufferedWriter skipfile = new BufferedWriter( new OutputStreamWriter(new FileOutputStream(skippedFiles),"UTF8") );//No I18N
                    int noOfFilesSkipped = skippedFilesList.size();
                    for(int i = 0;i< noOfFilesSkipped ; i++){
                        skipfile.write(skippedFilesList.get(i).toString()+"\n"); // No I18N
                    }
                    skipfile.close();
                    //zipConfFiles(skippedFiles.getPath());
                }

            }
            /*}catch(Exception e){*/
            closeZip();
        }
        catch(Exception e){
            closeZip();
            int ij= ithzip;
            deleteAllZip(ij);
            logout.write("\nException while zipping backup file \n "  ); //No I18N
            logout.flush();
            throw e;
        }


        //closeZip();
    }
    /**
     * This method returns true if at least one file is present in the given directory
     * @param dir - directory path which has to be explored for the presence of file
     * @return hasfile - whether the given folder contains a file or not.
     */
    private boolean hasFile(String dir) {
        boolean hasfile = false;
        File d = new File(dir);
        if (d.isDirectory()) {
            String[] list = d.list();
            if (list != null) {
                int len = list.length;
                for (int i = 0; i < len; i++) {
                    File f = new File(d, list[i]);
                    hasfile = hasFile(f.getPath());
                    if (hasfile) {
                        break;
                    }

                }
            }
        } else {
            hasfile = true;
        }
        return hasfile;
    }

    /**
     * This function is used to delete all the .data files that are created if the backup fails.
     * @param i number of .data that are created
     */
    public void deleteAllZip(int i){
        int r=1;
        while(r<=i){
            fileObjZipout[r].delete();
            r++;
        }
        File directory=new File(backupDir);
        if(directory.isDirectory() && (directory.list().length == 0)){
            directory.delete();

        }
    }

    /**
     * Getting database connection
     */

    private Connection getConnection() throws Exception
    {
        String sep = File.separator;
        //String dsfile = System.getProperty( "server.dir" ) + sep + ".." + sep + "server" + sep + "default" + sep;//No I18N
        //String dsfile = System.getProperty( "server.dir" )  + sep + "server" + sep + "default" + sep;//No I18N
        String dsfile = SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"customer-config.xml";//No I18N



        if( dbserver == null )
        {
            if( new File(dsfile).isFile() )
            {
                dbserver = ConnectionUtil.getActiveDBName();

                return ConnectionUtil.getConnection(dbserver);
            }
            else
            {
                throw new Exception( "Data Source file not found : " + dsfile );//No I18N
            }
        }
        else
        {
            return ConnectionUtil.getConnection(dbserver);
        }
    }

    /**
     * This method will delete all temp files
     */
    private void deleteAllTempFiles() throws Exception
    {
        deleteDynmicTablesFile("ServiceCatalog");//No I18N
        new File ( backupDir + separator + "backup_info.xml" ).delete();//No I18N
        new File ( backupDir + separator + "filelist.txt").delete();//No I18N
        new File ( backupDir + separator + "skippedFiles.txt").delete();//No I18N
        for(int i=1; i<=fileAttachmentsNumber; i++){
            new File ( SDDataManager.getInstance().getRootDir() + separator + "FileAttachments"+ i +".zip" ).delete();//No I18N
        }
        new File ( SDDataManager.getInstance().getRootDir() + separator + "FileAttachmentPath.properties" ).delete();//No I18N
        if( tableNames != null )
        {
            int len = tableNames.size();
            for( int i=0; i<len; i++ )
            {
                new File ( backupDir + separator + tableNames.get(i).toString().toLowerCase() + ".sql").delete();//No I18N
            }

            int len1 = extendedTableListArray.size();
            for( int i=0; i<len1; i++ )
            {
                new File ( backupDir + separator + extendedTableListArray.get(i).toString().toLowerCase() + ".sql").delete();//No I18N
            }
        }
    }

    private void addToZip(  String file) throws Exception
    {
		/*ZipEntry ent = new ZipEntry ( new File(file).getName() );
		zipout.putNextEntry (ent);*/

        FileInputStream fileReader = new FileInputStream(new File(file));
        try{
            long entryFileLength=(new File(file)).length();
            addEntry(new File(file).getName(),fileReader,entryFileLength, new File(file));
        }
        catch(Exception e){
            logout.write("\nError in Adding File to Zip " + file); // No I18N
            throw e;
        }
        finally
        {
            fileReader.close();
        }
                /*try
		{
			byte[]data = new byte[1024];
			for (int j = 0;; j++)
			{
				int len = fileReader.read (data);
				if (len < 0)
				{
					break;
				}
				zipout.write (data, 0, len);
				zipout.flush ();
			}
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			fileReader.close();
		}*/
    }


    private HashMap getMetaData( String table_name ) throws Exception
    {
        HashMap hash = new LinkedHashMap();
        PreparedStatement stmt = null;
        try
        {
            stmt = conn.prepareStatement( "SELECT TABLE_ID FROM TableDetails where TABLE_NAME = '" + table_name + "'" );//No I18N
            ResultSet rs = stmt.executeQuery();
            psCount++;
            if( rs.next() )
            {
                stmt = conn.prepareStatement( "SELECT COLUMN_NAME, MAX_SIZE FROM ColumnDetails where TABLE_ID = '" + rs.getString(1) + "'  ORDER BY column_id asc" );//No I18N
                rs = stmt.executeQuery();
                psCount++;
                while(rs.next())
                {
                    hash.put(rs.getString(1).toLowerCase(), rs.getString(2));
                }
            }
        }
        catch( Exception e )
        {
            logout.write("\nException while getting metadata "); // No I18N
            throw e;
        }
        finally
        {
            if( stmt != null )
            {
                try{stmt.close();}catch(Exception ex){}
            }
        }
        return hash;
    }

    /**
     * @param table
     * @return void
     * This method will generate dump file for the requested table
     */
    private void dumpTable( String table, String pkColumn ) throws Exception
    {
        if(scharTableCols.containsKey(table))
        {
            dumpTableUsingLimit(table);
        }
        else if( pkColumn == null )
        {
            logout.write( table + "\n" );//No I18N

            dumpTableUsingLimit(table);
        }
        else
        {
            dumpTableUsingCriteria(table, pkColumn);
        }
    }

    private void dumpTableUsingCriteria( String table, String pkColumn ) throws Exception
    {
        int fileindex = 0;
        long size = 0;
        boolean isFirstWriteCompleted = false;
        String filename;
        boolean hasResultSet = true;
        int fetchSize = customFetchSizeMap.get(table.toLowerCase())!=null ? ((Integer) customFetchSizeMap.get(table.toLowerCase())).intValue() : defaultFetchSize;
        if( attempt >= 3 )
        {
            throw new Exception("Unable to get the data from [" + table + "] table");//No I18N
        }
        PreparedStatement cstmt = null;
        int count = 0;
        try
        {
            cstmt = conn.prepareStatement( "SELECT max(" + pkColumn + ") FROM " + table );//No I18N
            ResultSet rcount = cstmt.executeQuery();
            psCount++;
            if( rcount.next() )
            {
                count = rcount.getInt(1);
            }
        }
        catch(Exception e)
        {
            dbhealthstatus = false;
            attempt++;
            System.out.println("ERROR = *" + e.getMessage() + "* Query = " + "SELECT max(" + pkColumn + ") FROM " + table  );//No I18N
            conn = getConnection();
            dumpTableUsingLimit(table);
            return;
        }
        finally
        {
            if( cstmt != null )
            {
                cstmt.close();
            }
        }

        logout.write( table + " Max(pk) = " + count + "\n" );//No I18N

        PreparedStatement stmt = null;
        //SD-50869 :- huge data backup issue in mssql and pgsql
        //if( count > 1000 &&  "mysql".equals(dbserver) )
        //{
        stmt = conn.prepareStatement( "SELECT * FROM " + table + " where " + pkColumn + " <= " + fetchSize );//No I18N
        psCount++;
		/*}
		else
		{
			stmt = conn.prepareStatement( "SELECT * FROM " + table );//No I18N
		}*/

        ResultSet rs = null;
        try
        {
            rs = stmt.executeQuery();
        }
        catch(Exception ex)
        {
            attempt++;
            logout.write("\nError when fetching rows from table " ); // No I18N
            ex.printStackTrace(new PrintWriter(logout));
            System.out.println("ERROR = *" + ex.getMessage() + "*");//No I18N
            conn = getConnection();
            dumpTableUsingLimit(table);
            dbhealthstatus = false;
            return;
        }

        ResultSetMetaData metaData = rs.getMetaData();
        int colCount = metaData.getColumnCount();

        HashMap metadata = getMetaData(table);

        File backupfile = new File(backupDir + separator + table.toLowerCase() + ".sql");//No I18N
        backupfile.createNewFile();

        FileOutputStream fout = new FileOutputStream(backupfile);

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout,"UTF8"));//No I18N

        int max_col_size = 0;

        try
        {
            StringBuffer buffer = new StringBuffer();
            StringBuffer header = new StringBuffer();
            //Added double codes for column name OFFSET is keyword in postgresql
            String colName = metaData.getColumnName(1);
            if(colName.equals("OFFSET"))
            {
                colName = "\""+colName+"\"";
            }
            header.append("INSERT INTO " + table + " (" + colName);//No I18N

            String comma = ",";//No I18N
            for (int i=1; i<colCount; i++)
            {
                colName = metaData.getColumnName(i+1);
                if(colName.equals("OFFSET"))
                {
                    colName = "\""+colName+"\"";
                }
                header.append( comma + colName);
            }
            header.append(") VALUES\n");//No I18N

            int rows = fetchSize;

            String nullString = "NULL";//No I18N
            String bitType = "bit";//No I18N
            String falseString = "false";//No I18N
            String booleanTrue = "1";//No I18N
            String booleanFalse = "0";//No I18N
            String emptyString = "";//No I18N
            String singleQuote = "'";//No I18N
            String doubleQuote = "''";//No I18N
            String backSlash = "\\";//No I18N
            String i18nchar = "N'";//No I18N
            String parenthesis = "(";//No I18N
            String commaspace = ", ";//No I18N
            do
            {
                while (rs.next())
                {
                    hasResultSet = true;
                    buffer.append(parenthesis);
                    for (int i=0; i<colCount; i++)
                    {
                        if (i > 0)
                        {
                            buffer.append(commaspace);
                        }
                        Object value = rs.getObject(i+1);
                        if (value == null)
                        {
                            buffer.append(nullString);
                        }
                        else
                        {
                            String colData = null;
                            if( value instanceof java.sql.Clob )
                            {
                                java.sql.Clob clob = (java.sql.Clob)value;
                                java.io.BufferedReader read = (java.io.BufferedReader)clob.getCharacterStream();
                                String tmp = read.readLine();
                                while ( tmp != null )
                                {
                                    if( colData == null )
                                    {
                                        colData = tmp;
                                    }
                                    else
                                    {
                                        colData += newline + tmp;
                                    }
                                    tmp = read.readLine();
                                }

                                if( colData == null )
                                {
                                    //MSSQL will return null in case of data is empty
                                    //(NOT NULL) so here colData assigned as empty string
                                    colData = emptyString;
                                }
                            }
                            else
                            {
                                colData = value.toString();
                            }
                            if(isTextColumn(metaData.getColumnTypeName(i + 1)))
                            {
                                colData = format(colData);
                                colData = colData.replaceAll(newline,newlinechar);
                                try
                                {
                                    max_col_size  = Integer.parseInt(metadata.get(metaData.getColumnName(i + 1).toLowerCase()).toString());
                                }
                                catch(Exception e)
                                {
                                    max_col_size = 0;
                                }
                                int numberOfQuotes=0;
                                if( max_col_size > 0 && max_col_size < 4000 )
                                {
                                    if( colData.length() > max_col_size )
                                    {
                                        max_col_size = max_col_size - 1;

                                        int temp_size= max_col_size-1;
                                        //This code is for handling single quote issue . All cases are handled properly in format() function except when the (n-1)th character is single quote.(where n is maximum length of the column)
                                        // code starts
                                        //Iterate from the end of the string to find number of quotes present in the end
                                        while( colData.charAt(temp_size)=='\'')
                                        {
                                            temp_size = temp_size - 1;
                                            numberOfQuotes++;
                                        }


                                        colData = colData.substring(0, max_col_size);

                                        while( colData.endsWith(backSlash))
                                        {
                                            max_col_size = max_col_size - 1;
                                            colData = colData.substring(0, max_col_size);
                                        }
                                    }
                                    else
                                    {
                                        max_col_size = colData.length();

                                        while( colData.endsWith(backSlash) )
                                        {
                                            max_col_size = max_col_size - 1;
                                            colData = colData.substring(0, max_col_size);
                                        }
                                    }
                                }
                                // If the number of quotes is even , the string is not changed .
                                if(numberOfQuotes%2==0)
                                {
                                    buffer.append(i18nchar).append(colData).append(singleQuote);
                                }
                                // If the number of quotes is odd , an extra quote will be added .
                                else
                                {
                                    buffer.append(i18nchar).append(colData).append(singleQuote).append(singleQuote);
                                }
                            }
                            else if(isDateTimeColumn(metaData.getColumnTypeName(i + 1)))
                            {
                                //in mysql5.0, nanoseconds gets appended to datetime datatype which creates problem
                                //while restoring, So we are converting to desired format without nanoseconds.
                                if ("datetime".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//No I18N
                                    Date tempDate = formatter.parse(colData);
                                    colData = formatter.format(tempDate);
                                }
                                //in spanish lanaguage '-' in the date string causes some data truncation problem
                                //while restoring. problem is the default date format is yyyy-dd-mm if we removed the
                                //'-' then yyyymmdd date format we be considered as default.\
                                colData = colData.replaceAll("-","--D--");//No I18N
                                buffer.append(i18nchar + colData + singleQuote);
                            }
                            else if(isBooleanColumn(metaData.getColumnTypeName(i + 1)))
                            {
                                if( falseString.equals(colData) || booleanFalse.equals(colData) || "f".equals(colData))
                                {
                                    buffer.append(singleQuote + booleanFalse + singleQuote);
                                }
                                else
                                {
                                    buffer.append(singleQuote + booleanTrue + singleQuote);
                                }
                            }
                            else
                            {
                                buffer.append(colData);
                            }
                        }
                    }
                    buffer.append(");\n");//No I18N
                }
                rs.close();
                size = size + buffer.capacity();
                                
                                /* Size is checked for every set of 1000 rows except first set before writing into file and if the size cumulatively exceeds 1 GB( max size ), then new sql file for that table is created with the same table name followed by '_number' @author Ponnambalam P. */
                if(isFirstWriteCompleted)
                {
                    if(size<maxsizeForSql)
                    {
                        out.write(buffer.toString());
                        buffer = new StringBuffer();
                    }
                    else
                    {
                        out.close();
                        fout.close();
                        size = buffer.capacity();
                        fileindex++;
                        filename = table.toLowerCase() + "_" +fileindex;
                        backupfile = new File(backupDir + separator +filename+ ".sql");//No I18N
                        backupfile.createNewFile();
                        fout = new FileOutputStream(backupfile);
                        out = new BufferedWriter(new OutputStreamWriter(fout,"UTF8"));//No I18N
                        out.write(header.toString());
                        out.write(buffer.toString());
                        buffer = new StringBuffer();
                        extendedTableListArray.add(filename);
                    }
                }
                else
                {
                    if(buffer.length() != 0)                // to prevent writing the 'header' alone in the .sql file for empty tables.
                    {
                        out.write(header.toString());
                        out.write(buffer.toString());
                        buffer = new StringBuffer();
                        isFirstWriteCompleted = true;
                    }
                }

                //sd-56665: long gap in pkcolumn values will be skipped when empty result sets are fetched.
                if(!hasResultSet)
                {
                    stmt = conn.prepareStatement("SELECT min(" + pkColumn +") FROM " + table + " WHERE "+ pkColumn + " > " + rows); //No I18N
                    rs= stmt.executeQuery();
                    psCount++;
                    if(rs.next()){
                        int nextPkValue = (int) rs.getLong(1);
                        rows = nextPkValue - 1 ;
                    }
                    rs.close();
                }

                /**
                 * Criteria changed from count > 1000 to count >= 1000
                 * Issue :-( Throwing cann't use result set after closing the result set
                 * Root Cause :-( If the table entry contains exactly 1000 rows
                 * @author Murugesan K
                 */
                if( count >= fetchSize )
                {
                    if( rows == count )
                    {
                        break;
                    }
                    //Issue id : 7038864 -> OOM issue in mssql-jre 1.6 backup -start				
                    if(dbserver.equalsIgnoreCase("mssql") && psCount%5000==0)
                    {
                        logout.write("\n Closing the existing connection and creating a new one due to preparedstatement count"+psCount);//No I18N
                        if( conn != null && !conn.isClosed() )
                        {
                            try
                            {
                                conn.close();
                            }
                            catch(Exception e)
                            {

                                logout.write("\n Exception while closing the connection"+e.getMessage());//No I18N
                                e.printStackTrace();
                            }
                        }
                        conn = ConnectionUtil.getConnection(dbserver);
                        logout.write("\n Proceeding Backup with new connection");//No I18N

                    }
                    //Issue id : 7038864 -> OOM issue in mssql-jre 1.6 backup -end
                    int retrive_rows = fetchSize;

                    rows = rows + fetchSize;
                    int retrive_offset = rows;
                    if( rows > count )
                    {
                        rows = count;
                        retrive_rows = count % fetchSize;
                    }
                    stmt = conn.prepareStatement( "SELECT * FROM " + table + " where " + pkColumn + " <= " + retrive_offset + " AND " + pkColumn + " > " + (retrive_offset - fetchSize) );//No I18N
                    rs = stmt.executeQuery();
                    hasResultSet = false;
                    psCount++;
                }
            }while( rows <= count );
            out.flush();
        }
        //SD-29551: Problem while taking Backup, "Invalid state, the Connection object is closed".
        catch(SQLException sqle)
        {
            if(sqle.getMessage().contains("Connection object is closed")) {
                logout.write("\nERROR = *" + sqle.getMessage() + "*\n");//No I18N
                sqle.printStackTrace();

                try {
                    rs.close();
                    out.flush();
                    stmt.close();
                    fout.close();
                    out.close();
                    logout.write("\nBackup table in progress is "+table.toLowerCase()+".sql \n");//No I18N
                    new File ( backupDir + separator + table.toLowerCase() + ".sql").delete();
                    logout.write("\nSQL File deleted :: "+table.toLowerCase() + ".sql \n");//No I18N
                }
                catch(Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
                logout.write("\nIt looks like database connection get lost while taking the backup, and now application will try to establish the db connection and continue with the backup in two attempts with 10 sec intervals. Attempt "+attempt+". \n");//No I18N

                Thread.sleep(10000);
                attempt++;
                dbhealthstatus = false;
                conn = getConnection();
                logout.write("\nReestablishing the connection again. Attempt:"+attempt+". \n");//No I18N
                dumpTableUsingCriteria(table, pkColumn);
                return;
            }
            else {
                logout.write("\nERROR = *" + sqle.getMessage() + "*\n");//No I18N
                sqle.printStackTrace();
                throw sqle;
            }
        }
        catch(Exception e)
        {
            logout.write("\nError in backup using Crit "); // No I18N
            e.printStackTrace();
            throw e;
        }
        finally
        {
            stmt.close();
            try
            {
                fout.close();
                out.close();
            }
            catch(Exception ex){ex.printStackTrace();}
        }
        attempt = 1;
    }

    private void dumpTableUsingLimit( String table ) throws Exception
    {
        if( attempt >= 3 )
        {
            throw new Exception("Unable to get the data from [" + table + "] table");//No I18N
        }
        PreparedStatement cstmt = null;
        int count = 0;
        ResultSet rcount = null;
        try
        {
            cstmt = conn.prepareStatement( "SELECT count(*) FROM " + table );//No I18N
            psCount++;
            rcount = cstmt.executeQuery();
            if( rcount.next() )
            {
                count = rcount.getInt(1);
            }
        }
        catch(Exception e)
        {
            dbhealthstatus = false;
            attempt++;
            logout.write("\nERROR = *" + e.getMessage() + "*");//No I18N
            conn = getConnection();
            dumpTableUsingLimit(table);
            return;
        }
        finally
        {
            if(rcount !=null)
            {
                rcount.close();
            }
            if( cstmt != null )
            {
                cstmt.close();
            }
        }

        logout.write( table + " count = " + count + "\n" );//No I18N

        String comma = ",";//No I18N
        String colName1="";
        PreparedStatement stmt = null;
        if( count > 1000 && "mysql".equals(dbserver) )
        {
            stmt = conn.prepareStatement( "SELECT * FROM " + table + " order by 1 limit 1000 offset 0" );//No I18N
        }
        else
        {
            if(scharTableCols.containsKey(table))
            {
                ArrayList<String> scharCols = scharTableCols.get(table);
                HashMap metadata = getMetaData(table);
                int k=0;
                for(Object objname:metadata.keySet())
                {
                    k++;
                    String colName =objname.toString();
                    if(scharCols.contains(colName.toUpperCase()))
                    {
                        colName = getModifiedColumnForScharType(colName);
                    }
                    colName1+=colName;
                    if(k!=metadata.size())
                    {
                        colName1+=",";
                    }
                }
                stmt = conn.prepareStatement( "SELECT "+colName1+" FROM " + table );//No I18N
            }
            else
            {
                stmt = conn.prepareStatement( "SELECT * FROM " + table );//No I18N
            }
        }
        psCount++;
        ResultSet rs = null;
        try
        {
            rs = stmt.executeQuery();
        }
        catch(Exception ex)
        {
            attempt++;
            logout.write("\nERROR = *" + ex.getMessage() + "*");//No I18N
            conn = getConnection();
            dumpTableUsingLimit(table);
            dbhealthstatus = false;
            return;
        }
        ResultSetMetaData metaData = rs.getMetaData();
        int colCount = metaData.getColumnCount();

        HashMap metadata = getMetaData(table);

        File backupfile = new File(backupDir + separator + table.toLowerCase() + ".sql");//No I18N
        backupfile.createNewFile();

        FileOutputStream fout = new FileOutputStream(backupfile);

        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout,"UTF8"));//No I18N

        int max_col_size = 0;

        try
        {
            StringBuffer buffer = new StringBuffer();
            //Added double codes for column name OFFSET is keyword in postgresql
            String colName = metaData.getColumnName(1);
            if(colName.equals("OFFSET"))
            {
                colName = "\""+colName+"\"";
            }
            buffer.append("INSERT INTO " + table + " (" + colName);//No I18N

            for (int i=1; i<colCount; i++)
            {
                colName = metaData.getColumnName(i+1);
                if(colName.equals("OFFSET"))
                {
                    colName = "\""+colName+"\"";
                }
                buffer.append( comma + colName);
            }
            buffer.append(") VALUES\n");//No I18N

            int rows = 1000;

            String nullString = "NULL";//No I18N
            String bitType = "bit";//No I18N
            String falseString = "false";//No I18N
            String booleanTrue = "1";//No I18N
            String booleanFalse = "0";//No I18N
            String emptyString = "";//No I18N
            String singleQuote = "'";//No I18N
            String doubleQuote = "''";//No I18N
            String backSlash = "\\";//No I18N
            String i18nchar = "N'";//No I18N
            String parenthesis = "(";//No I18N
            String commaspace = ", ";//No I18N
            String schar="S'CHAR@N'"; //No I18N
            String schar_end="S'CHAR@N"; //No I18N
            do
            {
                while (rs.next())
                {
                    buffer.append(parenthesis);
                    for (int i=0; i<colCount; i++)
                    {
                        if (i > 0)
                        {
                            buffer.append(commaspace);
                        }
                        Object value = rs.getObject(i+1);
                        if (value == null)
                        {
                            buffer.append(nullString);
                        }
                        else
                        {
                            String colData = null;
                            if( value instanceof java.sql.Clob )
                            {
                                java.sql.Clob clob = (java.sql.Clob)value;
                                java.io.BufferedReader read = (java.io.BufferedReader)clob.getCharacterStream();
                                String tmp = read.readLine();
                                while ( tmp != null )
                                {
                                    if( colData == null )
                                    {
                                        colData = tmp;
                                    }
                                    else
                                    {
                                        colData += newline + tmp;
                                    }
                                    tmp = read.readLine();
                                }

                                if( colData == null )
                                {
                                    //MSSQL will return null in case of data is empty
                                    //(NOT NULL) so here colData assigned as empty string
                                    colData = emptyString;
                                }
                            }
                            else
                            {
                                colData = value.toString();
                            }
                            if(isTextColumn(metaData.getColumnTypeName(i + 1)))
                            {
                                colData = format(colData);
                                colData = colData.replaceAll(newline,newlinechar);
                                try
                                {
                                    max_col_size  = Integer.parseInt(metadata.get(metaData.getColumnName(i + 1).toLowerCase()).toString());
                                }
                                catch(Exception e)
                                {
                                    max_col_size = 0;
                                }
                                int numberOfQuotes=0;
                                if( max_col_size > 0 && max_col_size < 4000 )
                                {
                                    if( colData.length() > max_col_size )
                                    {
                                        max_col_size = max_col_size - 1;

                                        int temp_size= max_col_size-1;
                                        //This code is for handling single quote issue . All cases are handled properly in format() function except when the (n-1)th character is single quote.(where n is maximum length of the column)
                                        // code starts
                                        //Iterate from the end of the string to find number of quotes present in the end 
                                        while( colData.charAt(temp_size)=='\'')
                                        {
                                            temp_size = temp_size - 1;
                                            numberOfQuotes++;
                                        }

                                        colData = colData.substring(0, max_col_size);

                                        while( colData.endsWith(backSlash))
                                        {
                                            max_col_size = max_col_size - 1;
                                            colData = colData.substring(0, max_col_size);
                                        }
                                    }
                                    else
                                    {
                                        max_col_size = colData.length();

                                        while( colData.endsWith(backSlash))
                                        {
                                            max_col_size = max_col_size - 1;
                                            colData = colData.substring(0, max_col_size);
                                        }
                                    }
                                }
                                ArrayList<String> scharCols = scharTableCols.get(table);
                                // If the number of quotes is even , the string is not changed .
                                if(numberOfQuotes%2==0)
                                {
                                    //if the column is schar,then append the placeholders
                                    if(scharTableCols.containsKey(table) && scharCols.contains(metaData.getColumnName(i+1).toUpperCase()))
                                    {
                                        buffer.append(schar).append(colData).append(singleQuote).append(schar_end);
                                    }

                                    else
                                    {
                                        buffer.append(i18nchar).append(colData).append(singleQuote);
                                    }
                                }
                                // If the number of quotes is odd , an extra quote will be added .
                                else
                                {
                                    //if the column is schar,then append the placeholders
                                    if(scharTableCols.containsKey(table) && scharCols.contains(metaData.getColumnName(i+1).toUpperCase()))
                                    {
                                        buffer.append(schar).append(colData).append(singleQuote).append(schar_end);
                                    }
                                    else
                                    {
                                        buffer.append(i18nchar).append(colData).append(singleQuote).append(singleQuote);
                                    }
                                }
                            }
                            else if(isDateTimeColumn(metaData.getColumnTypeName(i + 1)))
                            {
                                //In mysql5.0, nanoseconds gets appended to datetime datatype which creates problem
                                //while restoring, So we are converting to desired format without nanoseconds.
                                if ("datetime".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                                    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    Date tempDate = formatter.parse(colData);
                                    colData = formatter.format(tempDate); //No I18N
                                }
                                //in spanish lanaguage '-' in the date string causes some data truncation problem
                                //while restoring. problem is the default date format is yyyy-dd-mm if we removed the
                                //'-' then yyyymmdd date format we be considered as default.
                                colData = colData.replaceAll("-","--D--");//No I18N
                                buffer.append(i18nchar + colData + singleQuote);
                            }
                            else if(isBooleanColumn(metaData.getColumnTypeName(i + 1)))
                            {

                                if( falseString.equals(colData) || booleanFalse.equals(colData) || "f".equals(colData))
                                {
                                    buffer.append(singleQuote + booleanFalse + singleQuote);
                                }
                                else
                                {
                                    buffer.append(singleQuote + booleanTrue + singleQuote);
                                }
                            }
                            else
                            {
                                buffer.append(colData);
                            }
                        }
                    }
                    buffer.append(");\n");//No I18N
                    out.write(buffer.toString());
                    buffer = new StringBuffer();
                }
                rs.close();

                /**
                 * Criteria changed from count > 1000 to count >= 1000
                 * Issue :-( Throwing cann't use result set after closing the result set
                 * Root Cause :-( If the table entry contains exactly 1000 rows
                 * @author Murugesan K
                 */
                if( count >= 1000 && "mysql".equals(dbserver) )
                {
                    if( rows == count )
                    {
                        break;
                    }

                    int retrive_rows = 1000;

                    rows = rows + 1000;
                    int retrive_offset = rows;
                    if( rows > count )
                    {
                        rows = count;
                        retrive_rows = count % 1000;
                    }
                    stmt = conn.prepareStatement( "SELECT * FROM " + table + " order by 1 limit " + retrive_rows + " offset " + (retrive_offset - 1000) );//No I18N
                    rs = stmt.executeQuery();
                    psCount++;
                }
            }while( rows <= count && "mysql".equals(dbserver) );
            out.flush();
        }
        //SD-29551: Problem while taking Backup, "Invalid state, the Connection object is closed".
        catch(SQLException sqle)
        {
            if(sqle.getMessage().contains("Connection object is closed")) {
                logout.write("\nERROR = *" + sqle.getMessage() + "* \n");//No I18N
                sqle.printStackTrace();

                try {
                    rs.close();
                    out.flush();
                    stmt.close();
                    fout.close();
                    out.close();

                    System.out.println("Backup table in progress is "+table.toLowerCase()+".sql \n");
                    logout.write("\nBackup table in progress is "+table.toLowerCase()+".sql \n"); // No I18N
                    new File ( backupDir + separator + table.toLowerCase() + ".sql").delete();
                    logout.write("\nSQL File deleted :: "+table.toLowerCase() + ".sql \n"); // No I18N
                }
                catch(Exception ex) {
                    ex.printStackTrace();
                    throw ex;
                }
                //below comment will show in the console.
                System.out.println("It looks like database connection get lost while taking the backup, and now application will try to establish the db connection and continue with the backup in two attempts with 10 sec intervals. Attempt "+attempt+". \n");
                // below comment will be added in the SDPBackUp.log
                logout.write("\nIt looks like database connection get lost while taking the backup, and now application will try to establish the db connection and continue with the backup in two attempts with 10 sec intervals. Attempt "+attempt+". \n"); // No I18N

                Thread.sleep (10000);
                attempt++;
                dbhealthstatus = false;
                conn = getConnection();
                System.out.println("Reestablishing the connection again. Attempt :"+attempt+". \n");
                logout.write("\nReestablishing the connection again. Attempt:"+attempt+". \n"); // No I18N
                dumpTableUsingLimit(table);
                return;
            }
            else {
                logout.write("\nERROR = *" + sqle.getMessage() + "*\n");//No I18N
                sqle.printStackTrace();
                throw sqle;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            stmt.close();
            try
            {
                fout.close();
                out.close();
            }
            catch(Exception ex){ex.printStackTrace();}
        }
        attempt = 1;
    }

    /**
     * @param columnType
     * @return true/false
     * This method will check the given column eighter text or not
     */
    private static boolean isTextColumn( String columnType )
    {
        boolean flag = false;
        if( "varchar".equalsIgnoreCase(columnType) || "nvarchar".equalsIgnoreCase(columnType))
        {
            flag = true;
        }
        else if( "char".equalsIgnoreCase(columnType) || "nchar".equalsIgnoreCase(columnType))
        {
            flag = true;
        }
        else if( "text".equalsIgnoreCase(columnType) || "ntext".equalsIgnoreCase(columnType) || "citext".equalsIgnoreCase(columnType))
        {
            flag = true;
        }
        return flag;
    }

    private static boolean isDateTimeColumn(String columnType)
    {
        boolean flag = false;
        if( "datetime".equalsIgnoreCase(columnType) || "date".equalsIgnoreCase(columnType) || "timestamp".equalsIgnoreCase(columnType))
        {
            flag = true;
        }
        return flag;
    }
    private static boolean isBooleanColumn(String columnType)
    {
        boolean flag = false;
        if( "boolean".equalsIgnoreCase(columnType) || "bool".equalsIgnoreCase(columnType) || "bit".equalsIgnoreCase(columnType) || "tiny".equalsIgnoreCase(columnType))
        {
            flag = true;
        }
        return flag;
    }


    private String format( String colData )
    {
        StringBuffer buffer = new StringBuffer();
        StringCharacterIterator iterator = new StringCharacterIterator(colData);
        char character =  iterator.current();

        while (character != CharacterIterator.DONE )
        {
            if (character == '\'')
            {
                buffer.append("''");//No I18N
            }
            else if( character == '\\' )
            {
                if( "mysql".equals(dbserver) )
                {
                    buffer.append("\\\\");//No I18N
                }
                else
                {
                    buffer.append("\\");//No I18N
                }
            }
            else if( character == '\r' )
            {
                if( "mysql".equals(dbserver) )
                {
                    buffer.append("\\r");//No I18N
                }
                else
                {
                    buffer.append(crglinechar);//No I18N
                }
            }
            else if( character == '\n' )
            {
                if( "mysql".equals(dbserver) )
                {
                    buffer.append("\\n");//No I18N
                }
                else
                {
                    buffer.append(newlinechar);
                }
            }
            else if( (int)character == 0 )
            {
                /**
                 * Ignoring ^@ char
                 */
            }
            else
            {
                buffer.append(character);
            }
            character = iterator.next();
        }
        return buffer.toString();
    }

    /**
     * @param build_number
     * @return void
     * Create new xml file for containing backup db details
     */
    private void createBuildInfoXml( String build_number ) throws Exception
    {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        Document document = docBuilder.newDocument();

        Element root = document.createElement("backup-info");//No I18N
        document.appendChild(root);

        Date dateObj = new Date();

        Element backup_date = document.createElement("backup-date");//No I18N
        backup_date.appendChild(document.createTextNode(dateObj.toString()));
        root.appendChild(backup_date);

        String dbname = ConnectionUtil.getDataBase();

        Element db = document.createElement("database");//No I18N
        db.appendChild(document.createTextNode(dbname));//No I18N
        root.appendChild(db);
        logout.write("\ndatabase name : "+dbname + "\n"); // No I18N

        // SD-13894  Unable to restore the backup data across the databases.
        // Entry in backupinfo.xml whether data is mssql or mysql while taking backup
        dbserver = ConnectionUtil.getActiveDBName();

        Element dbtype=document.createElement("db-server");//No I18N
        //Element dbtype=document.createElement("DB-Server");//No I18N
        dbtype.appendChild(document.createTextNode(dbserver));
        root.appendChild(dbtype);


        Element build_num = document.createElement("build-number");//No I18N
        build_num.appendChild(document.createTextNode(build_number));
        root.appendChild(build_num);

        Element number_of_table = document.createElement("number-of-table");//No I18N
        number_of_table.appendChild(document.createTextNode(tableNames.size() + ""));//No I18N
        root.appendChild(number_of_table);

        //Partial Restore changes start
        Element backupid = document.createElement("backupid"); //No I18N
        backupid.appendChild(document.createTextNode(Long.toString(dateObj.getTime())));
        root.appendChild(backupid);
        //Partial Restore changes end

        String build_info = backupDir + separator + "backup_info.xml";//No I18N

        System.out.println("backup_info.xml        : " + new File(build_info).createNewFile());//No I18N
        System.out.println("-------------------------------------------------------------");//No I18N

        Source xmlSource = new DOMSource(document);
        Result result = new StreamResult(new FileOutputStream(build_info));

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty("indent", "yes");//No I18N

        transformer.transform(xmlSource, result);
    }

    /**
     * @return return backup file name
     */
    public String getBackupFileName( String build_number ) throws Exception
    {
        if( backupFileName == null )
        {
            //it360 chgs start here
            String backupType = System.getProperty("sdp.backup.type","dummy");//No I18N
            //it360 chgs end here
            String date = new java.text.SimpleDateFormat("MM_dd_yyyy_HH_mm").format( new Date().getTime());//No I18N

            if( backupType !=null && backupType.indexOf("--trim") >= 0 )
            {
                return "backup_" + dbserver + "_" + build_number + "_database_" + date + "_part_1.data";//No I18N
            }
            else if(update){
                return "backup_" + dbserver + "_" + build_number + "_fullbackup_upgrade_" + date + "_part_1.data";//No I18N
            }
            else if( attachment != null && database != null && attachment.equals("true") && database.equals("true"))
            {
                return "backup_" + dbserver + "_" + build_number + "_fullbackup_" + date + "_part_1.data";//No I18N
            }
            else if( attachment != null && attachment.equals("true") )
            {
                return "backup_" + dbserver + "_" + build_number + "_attachment_" + date + "_part_1.data";//No I18N
            }
            else if( database != null && database.equals("true") )
            {
                return "backup_" + dbserver + "_" + build_number + "_database_" + date + "_part_1.data";//No I18N
            }
            //it360 chgs start here
            else if( backupType !=null && backupType.equals("--confiles") )
            {
                return "backup_"  + build_number + "_conbackup_" + date + "_part_1.data";//No I18N
            }
            //it360 chgs end here
            else
            {
                return "backup_" + dbserver + "_" + build_number + "_fullbackup_" + date + "_part_1.data";//No I18N
            }
        }
        else
        {
            return backupFileName;
        }
    }

    /**
     * @return return backup directory path
     */
    public String getBackupDir() throws Exception
    {
        backupDir = System.getProperty("sdp.backup.home"); //No I18N
        if( backupDir == null )
        {
            if( schedule_backdir == null )
            {
                //backupDir = System.getProperty("server.dir") + separator + ".." + separator + "backup";//No I18N
                backupDir = SDDataManager.getInstance().getRootDir() + separator + "backup" +separator+ backupFileName.substring(0,backupFileName.lastIndexOf("_part_"));//No I18N
            }
            else
            {
                backupDir = schedule_backdir + separator + backupFileName.substring(0,backupFileName.lastIndexOf("_part_")) ;//No I18N
            }
        }
        else{
            backupDir = System.getProperty("sdp.backup.home") +separator+ backupFileName.substring(0,backupFileName.lastIndexOf("_part_"));//No I18N
        }
        //if((System.getProperty("sdp.backup.home")).contains("trimmedbackup")){
        //   backupDir = System.getProperty("sdp.backup.home") + separator+ backupFileName.substring(0,backupFileName.lastIndexOf("."));//No I18N
        //}

        if (!new File(backupDir).exists ())
        {
            //logout.write ("backupDir "+ backupDir +" does not exists. Going to create directory\n");//No I18N
            new File(backupDir).mkdirs();
            logout.write ("\nBackupDirectory : " + backupDir + " is created\n");//No I18N
        }

        return backupDir;
    }

    public void zipDirectory (String dir) throws Exception
    {
        File d = new File (dir);
        if (!d.exists ())
        {
            System.out.println ("Exiting $^$%^&$%^&$%^");//No I18N

        }else{
            if (!d.isDirectory ())
            {
                throw new IllegalArgumentException ("Compress: not a directory:  " + dir);//No I18N
            }
            String[]entries = d.list ();
		/*byte[]buffer = new byte[4096];	// Create a buffer for copying
		int bytes_read;*/
            if (entries != null) {  //sd-51346 : Backup stopped when any folder got corrupted.
                int len = entries.length;
                for (int i=0; i<len; i++ )
                {
                    File f = new File (d, entries[i]);
                    long alength=f.length();
                    System.out.println ("File added to zip " + f);//No I18N
                    if (f.isDirectory ())
                    {
                        zipDirectory (f.getPath ());
                    }
                    else
                    {try{
                        FileInputStream in = new FileInputStream (f);
                        try
                        {
                            String absolutePath = f.getPath ();
                            int startIndx = absolutePath.indexOf ("fileAttachments");//No I18N
                                        /* Code to include blog folder in backup zip. Blog folder contains
                                         * customerInfo.txt which holds Product DID. Do not move this code beneath
                                         * including custom folder in backup since both customerInfo.txt and custom
                                         * folder paths contain 'custom'
                                         */
                            if(startIndx == -1)
                            {
                                startIndx = absolutePath.indexOf("blog");//No I18N
                            }
                                        /* Code to include blog folder end */
                            if(startIndx == -1)
                            {
                                startIndx = absolutePath.indexOf ("custom");//No I18N
                            }
                            if(startIndx == -1)
                            {
                                startIndx = absolutePath.indexOf ("inlineimages");//No I18N
                            }
                            if(startIndx == -1)
                            {
                                startIndx = absolutePath.indexOf ("archive");//No I18N
                            }
                            if(startIndx == -1)
                            {

                                startIndx = absolutePath.indexOf ("LuceneIndex");//No I18N
                            }
                            if(startIndx == -1)
                            {
                                startIndx = absolutePath.indexOf("integration");//No I18N
                            }
                            if( startIndx == -1 )
                            {
                                startIndx = absolutePath.indexOf("app_relationships");//No I18N
                            }
                            if(startIndx == -1)
                            {
                                startIndx = absolutePath.indexOf("webremoterecordedfiles");//No I18N
                            }
                            String subStr = absolutePath.substring (startIndx);
                            addEntry(subStr,in,alength, f);
					/*ZipEntry entry = new ZipEntry (subStr);
					zos.putNextEntry (entry);
					while((bytes_read = in.read(buffer)) != -1)
					{
						zos.write(buffer, 0, bytes_read);
					}*/
                        }
                        catch(Exception e)
                        {
                            logout.write("\nError while zipping the directory/file "+f); // No I18N
                            throw e;
                        }
                        finally
                        {
                            in.close ();
                        }
                    }
                    catch(Exception e)
                    {
                        if(e.getMessage().contains("cannot access") || e.getMessage().contains("Access is denied")) {
                            logout.write("\n"+d+separator +entries[i] + "\n"); //No I18N
                            e.printStackTrace(new PrintWriter(logout));
                            skippedFilesList.add(f.getCanonicalPath());
                        }else{
                            throw e;
                        }
                    }
                    }
                }
            }

        }
    }
    /*
     * SD-24708, Configuration files should also be included while taking the trimmed-backup
     */
    public void zipConfFiles(String dir) throws Exception {
        File d = new File(dir);
        if (!d.exists()) {
            System.out.println("Exiting $^$%^&$%^&$%^");//No I18N

        } else {
            if (!d.isFile()) {
                throw new IllegalArgumentException("Compress: not a File:  " + dir);//No I18N
            }

            File f = new File(d.getCanonicalPath().toString());
            long alength = f.length();
            System.out.println("File added to zip " + f);//No I18N

            FileInputStream in = new FileInputStream(f);
            try {
                String absolutePath = f.getPath();
                if(absolutePath.substring(0).endsWith("customer-config.xml")) //No I18N
                {
                    absolutePath=absolutePath.replaceFirst("conf", "referenceFiles"); //No I18N
                }
                int startIndx = absolutePath.indexOf("bin");//No I18N

                if( startIndx != -1 )
                {
                    startIndx = absolutePath.indexOf("bindings");//No I18N

                    if( startIndx != -1 )
                    {
                        startIndx = absolutePath.indexOf("server");//No I18N
                    }
                    else
                    {
                        startIndx = absolutePath.indexOf("bin");//No I18N
                    }
                }

                if (startIndx == -1) {
                    startIndx = absolutePath.indexOf("fos");//No I18N
                }
                if(startIndx==-1)
                {
                    startIndx = absolutePath.indexOf ("zreports");//No I18N
                }
                if(startIndx == -1) {
                    startIndx = absolutePath.indexOf("referenceFiles");//No I18N
                }
                //SD-69232 Backup process server.xml is not inside the conf folder.
                if(startIndx == -1) {
                    startIndx = absolutePath.indexOf("conf");//No I18N
                }
                if (startIndx == -1)
                {
                    startIndx = absolutePath.indexOf("server");//No I18N
                }
                if (startIndx == -1)
                {
                    startIndx = absolutePath.indexOf("applications");//No I18N
                }

                String subStr = absolutePath.substring(startIndx);     // Adding Index to Folders going to Zip.

                addEntry(subStr, in, alength, f);
                /*ZipEntry entry = new ZipEntry (subStr);
					zos.putNextEntry (entry);
					while((bytes_read = in.read(buffer)) != -1) //Zipping Conf. files
					{
						zos.write(buffer, 0, bytes_read);
                }*/
            } catch (Exception e) {
                logout.write("\nError while zip conffile " + f ); // No I18N
                e.printStackTrace();
                throw e;
            } finally {
                in.close();
            }
        }
    }


    /**
     * Find the active DB server
     * @return Active DB server
     */
	/*private String getActiveDBServer() throws Exception
	{
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		docBuilder.setEntityResolver(new EntityResolver(){
			public InputSource resolveEntity(String publicId, String systemId )
			{
				try
				{
					new File("localhost.dtd").createNewFile();//No I18N
					return new InputSource(new FileInputStream("localhost.dtd"));//No I18N
				}
				catch( Exception e )
				{
					 e.printStackTrace();
				}
				return null;
			}
		});

                
                String dsfile=SDDataManager.getInstance().getRootDir()+"datasource-configuration.xml"; //No I18N
		Document doc = docBuilder.parse(dsfile);

		NodeList adapter = doc.getElementsByTagName("adapter");//No I18N
		Element element = (Element)adapter.item(0);

		return element.getAttribute("name");//No I18N
	}*/
        /*
         *This function will add all the files to zip and decides whether backup file should be splitted based on size
         *@param subStr - path string of the entry to be added
         *@param in - pointing to the entry
         *@param alength - length of the file to be added
         */
    private void addEntry(String subStr, FileInputStream in, long entryFileLength, File entryFile) throws Exception {


        byte[] buffer = new byte[4096];
        // Create a buffer for copying
        int bytes_read;
        backupFileSize = fileObjZipout[ithzip].length();
        if (isNetworkAttachment) {
            int tenMB = 10 * 1024 * 1024; //=10MB. FileAttchments will be 10MB less than the .data file size. If no reduction is provided, then new .data is created to add the fileAttchments.zip.
            if (((maxfilesize - (tenMB)) <= (backupFileSize + entryFileLength))) {

                filenamecount++;
                logout.write("\nZipfile size is :"+backupFileSize+ " bytes.\nExceeding the specified size ("+ (maxfilesize - (tenMB)) +" bytes) when adding "+subStr+"\nAdding new zipfile"); //No I18N
                zipout[ithzip].flush();
                zipout[ithzip].finish();
                zipout[ithzip].close();
                ithzip = initZipOut(SDDataManager.getInstance().getRootDir() + separator + "FileAttachments" + filenamecount + ".zip"); //No I18N
                //new ZipOutputStream(new FileOutputStream( backupDir + separator + "fileattachment"+filenamecount+backupFileName ));
            }

        } else {
            if ((maxfilesize <= (backupFileSize + entryFileLength))) {

                filenamecount++;
                logout.write("\nZipfile size is :"+backupFileSize+ "Exceeding the specified size ("+maxfilesize+") on adding "+subStr+"\nAdding new zipfile"); //No I18N
                ithzip = initZipOut(backupDir + separator + backupFileName.substring(0, backupFileName.lastIndexOf("_part_")) + "_part_"+ filenamecount + ".data");//No I18N
            }

        }
        if(entryFileLength == 0L){
            setDirectoryForFileInZipFile(subStr);
            zipout[ithzip].putNextEntry(entryFile, toCompressFileParams);
            if((bytes_read = in.read(buffer)) != -1){
                zipout[ithzip].write(buffer, 0, bytes_read);
            }
            zipout[ithzip].closeEntry();
            setDirectoryForFileInZipFile("");
        }else{

            if((bytes_read = in.read(buffer)) != -1){
                setDirectoryForFileInZipFile(subStr);
                zipout[ithzip].putNextEntry(entryFile, toCompressFileParams);
                zipout[ithzip].write(buffer, 0, bytes_read);
                while ((bytes_read = in.read(buffer)) != -1) {
                    zipout[ithzip].write(buffer, 0, bytes_read);
                }
                zipout[ithzip].closeEntry();
                setDirectoryForFileInZipFile("");
            }
        }

    }
    /*
     *This function initializes the new backupfile created and returns the index of the array of the ZipOutputStream
     * @param str - name of the backup file that is created
     */
    private int initZipOut(String str) throws Exception {
        ithzip++;
        backupFileSize = 0;
        //System.out.println("file");
        zipout[ithzip] = new ZipOutputStream(new FileOutputStream(new File(str)));
        logout.write("\nZipfile created: "+ str ); //No I18N
        fileObjZipout[ithzip] = new File(str);
        //System.out.println("zipout");
        try {

            if (!isNetworkAttachment) {
                byte[] b = new byte[1024];

                String strsub = str.substring(str.lastIndexOf(separator) + 1, str.length());

                b = strsub.getBytes();
                int len = strsub.length();
                if (filenametoschedule != null) {
                    filenametoschedule += ","; //No I18N
                    filenametoschedule += strsub;
                } else {
                    filenametoschedule = strsub;
                }
                fileList.write(b, 0, len);
                fileList.flush();
                // byte[] h = new byte[1024];
                fileList.write(System.getProperty("line.separator").toString().getBytes());
            }
        } catch (Exception e) {
            logout.write("\nError from initZip"); // No I18N
            throw e;
        }

        return ithzip;

    }
    /*
     *This closes the ZipOutputStream created so far and writes the filelist.txt in all the backup files
     */
    private void closeZip() throws Exception {
        fileList.close();
        int i = 1;
        File skipfile = new File(backupDir + separator + "skippedFiles.txt");
        if(skipfile.exists()){
            skipFileExists = true;
        }
        while (i <= ithzip) {
            FileInputStream fileList1 = new FileInputStream(new File(backupDir + separator + "filelist.txt"));
            try {
                addToAllZip(fileList1,backupDir + separator + "filelist.txt",i); //No I18N

            } catch (Exception e) {
                logout.write("\nException while closing zip" + e); // No I18N
                throw e;

            } finally {
                fileList1.close();
            }
            if(skipfile.exists()){

                FileInputStream skippedFiles = new FileInputStream(skipfile);
                try{
                    addToAllZip(skippedFiles,backupDir + separator + "skippedFiles.txt",i); //No I18N
                }catch(Exception e ){
                    logout.write("\n Exception while closing zip " +  e); // No I18N
                    throw e;
                }finally {
                    skippedFiles.close();
                }
            }
            zipout[i].finish();
            zipout[i].close();
            i++;
        }
    }
    /**
     * This function will add the files that are passed as parameters to all the .data files. Files related to that backup process are added in all the .data files (like filelist.txt...).
     * @param fis - fileInputStream of the file to be added. 
     * @param fileName - exact name of how the file should be in the .data .
     * @throws Exception
     */
    private void addToAllZip(FileInputStream fis,String fileName,int j) throws Exception
    {
        byte buffer[] = new byte[1024];
        int buf_length;
        File entryFile = new File(fileName);
        zipout[j].putNextEntry(entryFile, toCompressFileParams);
        while ((buf_length = fis.read(buffer)) != -1) {
            zipout[j].write(buffer, 0, buf_length);
        }
        zipout[j].closeEntry();
    }
    /*
     *This function checks whether the mentioned size of backup file is within limits. If not default 1GB is set as file size
     */
    private void checkBackupFileSize() throws Exception
    {
        long maxsize =  1536 * 1024 * 1024;
        int minsize = 500 * 1024 * 1024;
        if((maxfilesize < minsize) || (maxfilesize > maxsize)){
            maxfilesize = 1024 * 1024 * 1024;
            //System.out.println("The specified size is not with in the limits. So, default size(1GB) is assigned to the backup file size");
        }
    }


    /**
     * This function will return the keystore file name by reading server.xml
     * @return string keystore file name
     * @throws Exception
     */
    private String getKeystoreFileName() throws Exception{
        String value = null;
        try {
            File serverFile = new File(SDDataManager.getInstance().getRootDir()+"conf"+File.separator+"server.xml");//No I18N
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = VulnerabilityUtil.getInstance().applyXXEFix(dbFactory);
            Document doc = dBuilder.parse(serverFile);
            NodeList nList = doc.getElementsByTagName("Connector");
            if (nList.getLength()  > 0) {
                Node node = nList.item(0);
                Element eElement = (Element) node;
                if (eElement.hasAttribute("keystoreFile")) {
                    value = eElement.getAttribute("keystoreFile");
                    value = value.substring(value.lastIndexOf("/") + 1);
                }
            }
        } catch (Exception e) {
            System.out.println("Exception while fetching the keystore file name. Continuing with further process.");  //No I18N
            logout.write("\nException while fetching the file name of keystore" + e); //No I18N
        }
        return value;
    }

    private void printError(Exception e) throws Exception{
        logout.write("\nError during backup is :\n\n"); // No I18N
        e.printStackTrace(new PrintWriter(logout));
        System.out.println("Some error occured during backup. Please refer SDPbackup.log file from bin directory");
    }

    //method to get the schar tables and columns. SChar tables and columns gets stored in scharTableCols HashMap.
    private HashMap getScharTableCols()throws Exception
    {
        PreparedStatement pstmt = null;
        ResultSet rs1 = null;
        String tempTableName="",tempColName="";
        try
        {
            pstmt = conn.prepareStatement( "select * from TableDetails td join ColumnDetails cd on td.TABLE_ID=cd.TABLE_ID where cd.DATA_TYPE='SCHAR'");//No I18N
            rs1 = pstmt.executeQuery();
            while( rs1.next() )
            {
                tempTableName=rs1.getString("table_name");
                tempColName=rs1.getString("column_name");
                if(!scharTableCols.containsKey(tempTableName))
                {
                    scharTableCols.put(tempTableName,new ArrayList<String>());
                }
                scharTableCols.get(tempTableName).add(tempColName);
            }
        }
        catch( Exception e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(rs1!=null)
            {
                try{rs1.close();}catch(Exception ex){}
            }
            if(pstmt!=null){
                try{pstmt.close();}catch(Exception ex){}
            }
        }
        return scharTableCols;
    }

    //method to parse the productconfig file and obtain ecTag,encryption.algo and encryption.s2kmode
    private HashMap getProductConfig() throws Exception
    {
        HashMap<String,String> hm=new HashMap<String,String>();
        try
        {
            String fileName = System.getProperty("server.home") + "/conf/Persistence/persistence-configurations.xml"; //No I18N
            ConfigurationParser parser = new ConfigurationParser(fileName);
            confNameVsValue.putAll(parser.getConfigurationValues());
            confNameVsProps.putAll(parser.getConfigurationProps());
            String val=(String)confNameVsValue.get("ECTag");
            CryptoUtil.setEnDecryptInstance(new EnDecryptImpl());
            val=CryptoUtil.decrypt(val);
            hm.put("ecTag",val);
            Properties props = (Properties) confNameVsProps.get("postgres");
            if (props != null)
            {
                val = props.getProperty("encryption.algo");
                hm.put("cipher", val);
                val = props.getProperty("encryption.s2kmode");
                hm.put("s2k-mode", val);

            }
        }
        catch(Exception e){
            e.printStackTrace();
            throw e;
        }
        return hm;
    }

    public static void main(String args[]) throws Exception {
//        Backup obj = new Backup();
//        try {
//            if (args.length > 0)   //In .bat or .sh file, --size=500M (>500M or <1.5GB) should be given as argument to Backup.class file. Size in MB only.
//            {
//                String backupSplitsize = args[0];
//                if ((backupSplitsize.endsWith("M")) && ((backupSplitsize.indexOf("--size")) != -1)) {
//                    String backupSplitsizeStr = backupSplitsize.substring((backupSplitsize.indexOf("=") + 1), backupSplitsize.indexOf("M"));
//                    Integer intsize = Integer.parseInt(backupSplitsizeStr);
//                    maxfilesize = (int) (intsize * 1024 * 1024);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        } finally {
//            try {
//                obj.process();
//                if(skipFileExists){
//                    System.out.println("======================================================================");
//                    System.out.println("Some files were skipped during this backup");
//                    System.out.println("Some portion of these files seems to be locked by other process(like Anti-virus software etc. ). ");
//                    System.out.println("List of the skipped files can be found in 'skippedFiles.txt' present in the .data file generated during this backup.");
//                    System.out.println("======================================================================");
//                    System.out.println("*********************************************************");
//                    System.out.println("           Backup status : PARTIAL ");
//                    System.out.println("*********************************************************");
//                }else{
//                    System.out.println("*********************************************************");
//                    System.out.println("           Backup completed Successfully");
//                    System.out.println("*********************************************************");
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("*********************************************************");
//                System.out.println("Some error occured during backup. Please refer SDPbackup.log file from bin directory");
//                System.out.println("*********************************************************");
//            }
//            System.exit(0);
//        }
    }

    public Backup() {

        try {
//            if (args.length > 0)   //In .bat or .sh file, --size=500M (>500M or <1.5GB) should be given as argument to Backup.class file. Size in MB only.
//            {
//                String backupSplitsize = args[0];
//                if ((backupSplitsize.endsWith("M")) && ((backupSplitsize.indexOf("--size")) != -1)) {
//                    String backupSplitsizeStr = backupSplitsize.substring((backupSplitsize.indexOf("=") + 1), backupSplitsize.indexOf("M"));
//                    Integer intsize = Integer.parseInt(backupSplitsizeStr);
//                    maxfilesize = (int) (intsize * 1024 * 1024);
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                process();
                if(skipFileExists){
                    System.out.println("======================================================================");
                    System.out.println("Some files were skipped during this backup");
                    System.out.println("Some portion of these files seems to be locked by other process(like Anti-virus software etc. ). ");
                    System.out.println("List of the skipped files can be found in 'skippedFiles.txt' present in the .data file generated during this backup.");
                    System.out.println("======================================================================");
                    System.out.println("*********************************************************");
                    System.out.println("           Backup status : PARTIAL ");
                    System.out.println("*********************************************************");
                }else{
                    System.out.println("*********************************************************");
                    System.out.println("           Backup completed Successfully");
                    System.out.println("*********************************************************");
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("*********************************************************");
                System.out.println("Some error occured during backup. Please refer SDPbackup.log file from bin directory");
                System.out.println("*********************************************************");
            }
            System.exit(0);
        }
    }

    //replace the SCHAR column name with appropritae decrpyt function.
    private String getModifiedColumnForScharType(String colName) throws Exception{
        String modifiedColName = null;
        if(colName != null && !"".equals(colName)) {
            if ("postgres".equals(dbserver)) {
                modifiedColName = "pgp_sym_decrypt(" + colName + ",'" + ecTag + "','cipher-algo=" + cipher + "')" + " as " + colName; //No I18N
            } else if ("mssql".equals(dbserver)) { //No I18N
                modifiedColName = " convert(NVARCHAR(4000),decryptbykeyautocert(cert_id('ZOHO_CERT'),NULL,\"" + colName + "\")) as " + colName; //No I18N
            }
        }
        return modifiedColName;
    }

    //zip4j changes START

    private void initializeZipFileParams() throws Exception {
        initializeZipFileParams(Zip4jConstants.COMP_DEFLATE, Zip4jConstants.DEFLATE_LEVEL_NORMAL);
    }

    private void initializeZipFileParams(int compressionMethod, int compressionLevel) throws Exception {
        initializeZipFileParams(compressionMethod, compressionLevel, true, Zip4jConstants.ENC_METHOD_AES, Zip4jConstants.AES_STRENGTH_256);
    }

    private void initializeZipFileParams(int compressionMethod, int compressionLevel, boolean toEncrypt, int encryptionMethod, int aesKeyStrength) throws Exception {
        toCompressFileParams = new ZipParameters();
        toCompressFileParams.setCompressionMethod(compressionMethod);
        toCompressFileParams.setCompressionLevel(compressionLevel);
        if (toEncrypt) {
            toCompressFileParams.setEncryptionMethod(encryptionMethod);
            if (encryptionMethod == Zip4jConstants.ENC_METHOD_AES && aesKeyStrength > 0) {
                toCompressFileParams.setAesKeyStrength(aesKeyStrength);
            }
            toCompressFileParams.setEncryptFiles(toEncrypt);
            if(!isScheduledBackup){
                getScheduledBackupConfiguredPasswordForBackup();
            }
            toCompressFileParams.setPassword(backupFilePassword);
        }
        logout.write("Zip params configured successfully"); //No I18N
    }

    private void setDirectoryForFileInZipFile(String filePath) throws Exception{
        int lastIndexOfFileSeparator = filePath.lastIndexOf(File.separator);
        if(lastIndexOfFileSeparator == -1){
            toCompressFileParams.setRootFolderInZip("");
        }
        else{
            toCompressFileParams.setRootFolderInZip(filePath.substring(0, lastIndexOfFileSeparator));
        }
    }

//    public String getScheduledBackupConfiguredPassword() throws Exception{
////        return getScheduledBackupConfiguredPassword(false);
//    }

    private String getScheduledBackupConfiguredPasswordForBackup() throws Exception{
        if(isScheduledBackup) {
            return "";
        }
        else{
            String passwordIdStr = null;
            java.sql.Statement stmt = null;
            ResultSet rs = null;
            try
            {
                conn = getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select PARAMVALUE from GlobalConfig where CATEGORY='SCHEDULE_BACKUP_PASSWORD'"); //No I18N
                if(rs.next()){
                    passwordIdStr = rs.getString(1);
                }
                if(passwordIdStr != null && !"".equals(passwordIdStr)){
                    String passwordQueryStr = "select " + getModifiedColumnForScharType("domainpassword") + " from passwordinfo where passwordid = " + Long.parseLong(passwordIdStr.trim()); //No I18N
                    rs = stmt.executeQuery(passwordQueryStr);
                    if(rs.next()){
                        backupFilePassword = rs.getString(1);
                        if(backupFilePassword == null || "".equals(backupFilePassword.trim())){
                            handlePasswordUnavailability();
                            throw new Exception("Empty password configured for backup.");
                        }
                        backupFilePassword = backupFilePassword.trim();
                    }
                }
                else{
                    handlePasswordUnavailability();
                    throw new Exception("No password configured for backup.");
                }
            }
            catch(Exception e)
            {
                logout.write("\nException while trying to fetch configured backup password."); //No I18N
                e.printStackTrace();
                throw e;
            }
            finally
            {
                if(rs!=null){
                    rs.close();
                }
                if( stmt != null )
                {
                    stmt.close();
                }
            }
            return backupFilePassword;
        }
    }

    private void handlePasswordUnavailability() throws Exception{
        String errorMessage = "Configure password to back up. To generate the default password, restart the application once."; //No I18N
        logout.write(errorMessage);
        displayErrorMessage(errorMessage);
    }

    private void displayErrorMessage(String message) throws Exception{
        try{
            displayErrorMessage(message, "D"); //No I18N
        }
        catch(Exception e){
            displayErrorMessage(message, "C"); //No I18N
        }
    }

    private void displayErrorMessage(String message, String invocationType) throws Exception{
        if("C".equalsIgnoreCase(invocationType)){
            ConsoleOut.print(message);
        }
        else if("D".equalsIgnoreCase(invocationType)){
            JOptionPane.showMessageDialog(null, message, "Backup Password Error", JOptionPane.ERROR_MESSAGE); //No I18N
        }
    }

	/*private void fetchPasswordForBackup() throws Exception{
    	try{
    		fetchPasswordForBackup("D"); //No I18N
		}
		catch(Exception e){
    		if(e.getMessage().contains("Cannot proceed with the backup process without password")){
    			throw e;
			}
    		fetchPasswordForBackup("C"); //No I18N
		}
	}

	private void fetchPasswordForBackup(String invocationType) throws Exception{
    	if("C".equalsIgnoreCase(invocationType)){
			//Get input from the console
			ConsoleOut.print("Enter password to extract the backup file: "); //No I18N
			backupFilePassword = System.console() != null ? new String(System.console().readPassword()).trim() : new BufferedReader(new InputStreamReader(System.in)).readLine().trim();
			if("".equals(backupFilePassword)){
				fetchPasswordForBackup("C"); //No I18N
			}
		}
		else if("D".equalsIgnoreCase(invocationType)){
			JPanel panel = new JPanel();
			panel.setLayout(new java.awt.BorderLayout());
			JLabel label = new JLabel("Enter password for the backup file:"); //No I18N
			label.setHorizontalAlignment(javax.swing.SwingConstants.LEFT);
			JPasswordField passwordField = new JPasswordField();
			passwordField.setMinimumSize(new java.awt.Dimension(44, 19));
			panel.add(label, BorderLayout.NORTH);
			panel.add(passwordField, BorderLayout.SOUTH);
			String[] options = new String[]{"Confirm"}; //No I18N
			int option = JOptionPane.showOptionDialog(null, panel, "Enter password", JOptionPane.NO_OPTION, JOptionPane.PLAIN_MESSAGE, null, options, options[0]); //No I18N
			if(option == -1){
				logout.write("\nCannot proceed with the backup process without password."); //No I18N
				if(update == true){
					throw new Exception("Cannot proceed with the backup process without password"); //No I18N
				}
				else{
					System.exit(0);
				}
			}
			if(option == 0)
			{
				backupFilePassword = new String(passwordField.getPassword()).trim();
				if("".equals(backupFilePassword)){
					fetchPasswordForBackup("D"); //No I18N
				}
			}
		}
	}*/

    public String getBackupFilePassword(){
        return backupFilePassword;
    }
    //zip4j changes END

}