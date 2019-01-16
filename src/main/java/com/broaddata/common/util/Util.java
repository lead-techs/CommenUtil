/*
 * Util.java
 */
package com.broaddata.common.util;

import org.apache.log4j.Logger;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.ResourceBundle;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.dom4j.Document;
import org.dom4j.Element;
import org.hibernate.exception.JDBCConnectionException;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Seconds;
import org.joda.time.Years;
import org.json.JSONObject;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.TypeMissingException;
import java.sql.Connection;
import java.sql.Statement;
import javax.persistence.Query;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.hibernate.exception.ConstraintViolationException;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DatabaseMetaData;
import java.util.Locale;

import com.broaddata.common.model.enumeration.ContentType;
import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.model.enumeration.MetadataIndexType;
import com.broaddata.common.model.enumeration.PlatformServiceType;
import com.broaddata.common.model.enumeration.RelativeTimeType;
import com.broaddata.common.model.enumeration.ScheduledRunType;
import com.broaddata.common.model.enumeration.SearchType;
import com.broaddata.common.model.enumeration.TimeAggregationType;
import com.broaddata.common.model.enumeration.TimeUnit;
import com.broaddata.common.model.enumeration.UserType;
import com.broaddata.common.model.enumeration.WeekDays;
import com.broaddata.common.model.organization.AnalyticDataview;
import com.broaddata.common.model.organization.Content;
import com.broaddata.common.model.organization.Dataobject;
import com.broaddata.common.model.organization.Datasource;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.organization.IndexSet;
import com.broaddata.common.model.organization.Repository;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.platform.DataobjectTypeAssociation;
import com.broaddata.common.model.platform.DatasourceType;
import com.broaddata.common.model.platform.LogFileDefinition;
import com.broaddata.common.model.platform.ServiceInstance;
import com.broaddata.common.model.platform.StructuredFileDefinition;
import com.broaddata.common.model.platform.UnstructuredFileProcessingType;
import com.broaddata.common.model.pojo.DataWorkerAdminData;
import com.broaddata.common.model.vo.DataobjectVO;
import com.broaddata.common.thrift.commonservice.CommonService;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.thrift.dataservice.DiscoveredDataobjectInfo;
import com.broaddata.common.thrift.dataservice.FrontendMetadata;
import com.broaddata.common.thrift.dataservice.Job;
import com.broaddata.common.thrift.dataservice.SearchRequest;
import com.broaddata.common.thrift.dataservice.SearchResponse;
import com.broaddata.common.model.enumeration.AssociationTypeProperty;
import com.broaddata.common.model.enumeration.BooleanOperatorType;
import com.broaddata.common.model.enumeration.ComputingNodeType;
import com.broaddata.common.model.enumeration.DataobjectStatus;
import com.broaddata.common.model.enumeration.OrganizationBusinessStatus;
import com.broaddata.common.model.enumeration.ParentChildSearchType;
import com.broaddata.common.model.enumeration.TimeFolderType;
import com.broaddata.common.model.organization.Catalog;
import com.broaddata.common.model.organization.DataApiCall;
import com.broaddata.common.model.organization.DatasourceEndJob;
import com.broaddata.common.model.organization.MetricsTagData;
import com.broaddata.common.model.organization.VideoAlertTask;
import com.broaddata.common.model.platform.MetricsDefinition;
import com.broaddata.common.model.platform.Organization;
import com.broaddata.common.model.platform.OrganizationBusinessDateInfo;
import com.broaddata.common.model.platform.PlatformMetrics;
import com.broaddata.common.model.platform.Program;
import com.broaddata.common.modelprocessor.ModelProcessor;
import com.broaddata.common.thrift.dataservice.Dataset;
import com.broaddata.common.thrift.dataservice.DatasetInfo;

public class Util 
{
    static final Logger log = Logger.getLogger("Util");

    private static final Locale defaultLocale = new Locale("zh", "CN");
    private static final Map<Integer, EntityManagerFactory> emfList = new HashMap<>();
    private static volatile EntityManagerFactory systemEmf = null;
    private static volatile EntityManagerFactory recEmf = null;
    public static String commonServiceIPs = null;
    private static final Lock lockForEm = new ReentrantLock();
    private static final Lock lockForPlatformEm = new ReentrantLock();
    public static Map<String, Object> objectCache = new HashMap<>();
    public static String mountStr = "";

    public static int getObjectArea(String topX,String topY,String bottomX,String bottomY)
    {
        int height = Integer.parseInt(bottomY)-Integer.parseInt(topY);
        int width = Integer.parseInt(bottomX) - Integer.parseInt(topX);
        return height * width;
    } 
    
    public static String saveFileToRepositoryObjectStorage(int organizationId, CommonService.Client commonService, DataService.Client dataService, int nodeOSType, ByteBuffer fileContentStream, int repositoryId) throws Exception 
    {
        ServiceInstance objectStorageServiceInstance = (ServiceInstance) Tool.deserializeObject(commonService.getObjStorageSI(organizationId, repositoryId).array());

        Map<String, String> objectStoreServiceProviderProperties = Util.getServiceInstancePropertyConfigMap(objectStorageServiceInstance.getConfig());

        String contentId = DataIdentifier.generateContentId(fileContentStream);

        ContentStoreUtil.storeContent(organizationId, dataService, nodeOSType, contentId, fileContentStream, objectStorageServiceInstance, objectStoreServiceProviderProperties);

        return contentId;
    }

    public static String savePictureToRepository(DataService.Client dataService, CommonService.Client commonService, int organizationId, String filename, ByteBuffer pictureContent, int faceRepositoryId) throws Exception {
        int sourceApplicationId = 0;
        int datasourceType = 1; // file server
        int datasourceId = 0;
        boolean needToExtractContentTexts = true;
        int edfRepositoryId = 3; // from faceRepositoryId
        String nodeOSType = "1";
        Map<String, String> metadata = new HashMap<>();

        metadata.put("file_name", filename);
        metadata.put("file_ext", "jpg");

        log.info("repositoryId=" + faceRepositoryId);
        String dataobjectId = DataIdentifier.generateDataobjectId(organizationId, sourceApplicationId, datasourceType, datasourceId, filename, edfRepositoryId);
        log.info("dataobjectId=" + dataobjectId);

        log.info("commonKeys=" + CommonKeys.DATAOBJECT_TYPE_FOR_FILE + "  dataobjectId=" + dataobjectId + " nodeOSType=" + nodeOSType + "  faceFileByte=" + pictureContent + "  sourceApplicationId=" + sourceApplicationId + "  datasourceType=" + datasourceType + "  repositoryId=" + edfRepositoryId + " needToExtractContentTexts=" + needToExtractContentTexts);

        //DataServiceUtil.storeVideoDataobject(CommonKeys.DATAOBJECT_TYPE_FOR_FILE, dataobjectId, Integer.parseInt(nodeOSType), ByteBuffer.wrap(pictureContent)., dataService, commonService, organizationId, sourceApplicationId, datasourceType, datasourceId, filename, filename, faceRepositoryId, needToExtractContentTexts, metadata);
        DataServiceUtil.storeVideoDataobject(CommonKeys.DATAOBJECT_TYPE_FOR_FILE, dataobjectId, Integer.parseInt(nodeOSType), pictureContent, dataService, commonService, organizationId, sourceApplicationId, datasourceType, datasourceId, filename, filename, faceRepositoryId, needToExtractContentTexts, metadata);
        return dataobjectId;
    }

    public static List<VideoAlertTask> getCameraVideoAlertTasks(EntityManager em, int cameraId) {
        String sql = String.format("from VideoAlertTask where cameraIds like '%%-%d-%%'", cameraId);
        List<VideoAlertTask> videoObjectTypeList = em.createQuery(sql).getResultList();
        return videoObjectTypeList;
    }

    public static String getNodeTypes(ComputingNode node) 
    {
        String nodeTypes = "";

        String[] vals = node.getNodeType().split("\\,");

        for (String val : vals) 
            nodeTypes += ComputingNodeType.findByValue(Integer.parseInt(val)).getTypeName() + ",";

        nodeTypes = nodeTypes.substring(0,nodeTypes.length()-1);

        return nodeTypes;
    }

    public static Object getDataBatchProgramBaseFromFile(String filepath, String className) throws Exception {
        try {
            File file = new File(filepath);

            if (!file.exists()) {
                throw new Exception("file not exists");
            }

            log.info(" classname=" + className);

            URL url = file.toURI().toURL();
            URLClassLoader loader = new URLClassLoader(new URL[]{url});
            Class clazz = loader.loadClass(className);

            log.info(" clazz =" + clazz);

            Class[] paramDef = new Class[]{};
            Constructor constructor = clazz.getConstructor(paramDef);
            Object[] params = new Object[]{};

            return constructor.newInstance(params);
        } catch (Exception e) {
            log.error(" getClassFromFile() faield! e=" + e);
            throw e;
        }
    }

    public static ModelProcessor getModelProcessor(String modelProcessorClass) throws Exception {
        try {
            Class processorClass = Class.forName(modelProcessorClass);
            Class[] paramDef = new Class[]{};
            Constructor constructor = processorClass.getConstructor(paramDef);
            Object[] params = new Object[]{};

            return (ModelProcessor) constructor.newInstance(params);
        } catch (Exception e) {
            log.error("getModelProcessor() failed! e=" + e);
            throw e;
        }
    }

    public static DataBatchProgramBase getDataBatchProgram(Program program) throws Exception {
        try {
            Class processorClass = Class.forName(program.getProgramName());
            Class[] paramDef = new Class[]{};
            Constructor constructor = processorClass.getConstructor(paramDef);
            Object[] params = new Object[]{};

            return (DataBatchProgramBase) constructor.newInstance(params);
        } catch (Exception e) {
            log.error("getDataBatchProgram() failed! e=" + e);
            throw e;
        }
    }

    public static void updateDataChannelInfo(EntityManager platformEm, int[] organizations, int lockResourceType, int dataChannel, String clientId) {
        EntityManager em = null;

        try {
            for (int organizationId : organizations) {
                Tool.SleepAWhile(1, 0);

                em = Util.getEntityManagerFactory(organizationId).createEntityManager();

                Repository configRrepository = Util.getConfigRepository(em, organizationId);
                Client configEsClient = ESUtil.getClient(em, platformEm, configRrepository.getId(), false);

                String resourceId = String.valueOf(dataChannel);
                String otherInfo = "";
                long version = ResourceLockUtil.updateLockInfo(configEsClient, organizationId, lockResourceType, resourceId, clientId, otherInfo);

                if (version < 0) {
                    log.error(" updateLockInfo failed! version=" + version);
                }
            }
        } catch (Exception e) {
            log.error(" updateDataChannelInfo() failed! e=" + e);
        } finally {
            if (em != null) {
                em.close();
            }
        }
    }

    public static int getNewDataChannel(int[] organizations, EntityManager platformEm, int lockResourceType, String clientId) {
        EntityManager em = null;

        try {
            for (int organizationId : organizations) {
                Tool.SleepAWhile(1, 0);

                em = Util.getEntityManagerFactory(organizationId).createEntityManager();

                Repository configRrepository = Util.getConfigRepository(em, organizationId);
                Client configEsClient = ESUtil.getClient(em, platformEm, configRrepository.getId(), false);

                String sql = String.format("select dataChannel,count(*) from DataobjectType where (organizationId=0 or organizationId=%d) and parentType>0 group by dataChannel", organizationId);
                List<Object[]> list = (List<Object[]>) platformEm.createQuery(sql).getResultList();

                for (Object[] objs : list) {
                    String resourceId = String.valueOf(objs[0]);
                    String otherInfo = "";
                    long version = ResourceLockUtil.getResourceLock(configEsClient, organizationId, lockResourceType, resourceId, clientId, otherInfo);

                    if (version < 0) {
                        continue;
                    }

                    log.info(" get lock version =" + version);

                    return Integer.parseInt(resourceId);
                }
            }
        } catch (Exception e) {
            log.error(" getNewDataChannel() failed! e=" + e);
        } finally {
            if (em != null) {
                em.close();
            }
        }

        return -1;
    }

    public static void loadDataChannelData(EntityManager platformEm, Map<Integer, Integer> dataChannelMap) {
        List<DataobjectType> dataobjectTypes = (List<DataobjectType>) platformEm.createQuery("from DataobjectType").getResultList();

        for (DataobjectType dataobjectType : dataobjectTypes) {
            dataChannelMap.put(dataobjectType.getId(), dataobjectType.getDataChannel());
        }
    }

    public static Dataset executeHBaseSQL(DatasourceConnection datasourceConnection, String sqlStr, String[] selectColumnNames, boolean needDatasetInfo, int startFrom, int maxExpectHits) throws Exception {
        List<Map<String, String>> rows;

        Dataset dataset = new Dataset();
        DatasetInfo datasetInfo;
        Map<String, Object> ret;

        log.info("excuting hbase sql=" + sqlStr);

        dataset.setRows(new ArrayList<Map<String, String>>());
        dataset.setTotalRow(0);
        dataset.setCurrentRow(dataset.getRows().size());
        dataset.setUpdateCount(-1);
        dataset.setMessage("");

        String hbaseIp;
        String hbasePort;
        String znodeParent;
        String familyName;

        try {
            hbaseIp = Util.getDatasourceConnectionProperty(datasourceConnection, "ip_address");
            hbasePort = Util.getDatasourceConnectionProperty(datasourceConnection, "port");
            znodeParent = Util.getDatasourceConnectionProperty(datasourceConnection, "znode_parent");
            familyName = Util.getDatasourceConnectionProperty(datasourceConnection, "default_family_name");

            HBaseUtil.initConn(hbaseIp, hbasePort, znodeParent, familyName);

            sqlStr = sqlStr.trim();

            ret = HBaseUtil.getRowDataBySQL(sqlStr, maxExpectHits);

            long count = (long) ret.get("count");
            rows = (List<Map<String, String>>) ret.get("rows");

            List<String> columnNames = new ArrayList<>();
            Map<String, Map<String, String>> columnInfoMap = new HashMap<>();

            columnNames.add("rowId");

            Map<String, String> columnInfo = new HashMap<>();

            columnInfo.put("type", String.valueOf(MetadataDataType.INTEGER.getValue()));
            columnInfo.put("description", "rowId");
            columnInfo.put("len", String.valueOf(4));
            columnInfo.put("precision", String.valueOf(0));

            if (needDatasetInfo) {
                columnInfoMap.put("rowId", columnInfo);
            }

            if (rows.size() > 0) {
                Map<String, String> row = rows.get(0);

                for (Map.Entry<String, String> column : row.entrySet()) {
                    String columnName = column.getKey();
                    String value = column.getValue();

                    if (value == null) {
                        value = "";
                    }

                    columnNames.add(columnName);
                    columnInfo = new HashMap<>();

                    columnInfo.put("type", String.valueOf(MetadataDataType.STRING.getValue()));
                    columnInfo.put("description", columnName);
                    columnInfo.put("len", String.valueOf(value.length() * 2));
                    columnInfo.put("precision", String.valueOf(0));

                    if (needDatasetInfo) {
                        columnInfoMap.put(columnName, columnInfo);
                    }
                }

                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);

                datasetInfo.setDatasetName("");
                datasetInfo.setDatasetSourceType(0);
                datasetInfo.setDatasetSource("");
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);
            }

            int k = 1;
            for (Map<String, String> row : rows) {
                row.put("rowId", String.valueOf(k));
                k++;
            }

            dataset.setRows(rows);

            dataset.setTotalRow(count);
            dataset.setCurrentRow(rows.size());

            return dataset;
        } catch (Exception e) {
            String error = " executeHBaseSQL() failed! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e);
            log.error(error);
            throw e;
        } finally {
            HBaseUtil.closeConn();
        }
    }

    public static Dataset executeSQL(DatasourceConnection datasourceConnection, String sqlStr, String[] selectColumnNames, boolean needDatasetInfo, int startFrom, int maxExpectHits) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;
        Dataset dataset = new Dataset();
        DatasetInfo datasetInfo;
        long totalRows = 0;

        conn = JDBCUtil.getJdbcConnection(datasourceConnection);
        stmt = conn.createStatement();

        log.info("excuting sql=" + sqlStr);

        sqlStr = sqlStr.trim();

        boolean hasResultSet = stmt.execute(sqlStr);

        dataset.setRows(new ArrayList<Map<String, String>>());
        dataset.setTotalRow(0);
        dataset.setCurrentRow(dataset.getRows().size());
        dataset.setUpdateCount(-1);
        dataset.setMessage("");

        if (hasResultSet == false) {
            int count = stmt.getUpdateCount();
            dataset.setUpdateCount(count);
            dataset.setMessage(String.format("%d rows affected", count));
        } else {
            resultSet = stmt.getResultSet();
            stmt.setFetchSize(1000);

            if (resultSet.next()) {
                List<String> columnNames = new ArrayList<>();
                Map<String, Map<String, String>> columnInfoMap = new HashMap<>();

                columnNames.add("rowId");

                Map<String, String> columnInfo = new HashMap<>();

                columnInfo.put("type", String.valueOf(MetadataDataType.INTEGER.getValue()));
                columnInfo.put("description", "rowId");
                columnInfo.put("len", String.valueOf(4));
                columnInfo.put("precision", String.valueOf(0));

                if (needDatasetInfo) {
                    columnInfoMap.put("rowId", columnInfo);
                }

                ResultSetMetaData rsmd = resultSet.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String columnName = rsmd.getColumnName(i);

                    if (columnName.toUpperCase().equals("COUNT") || columnName.toUpperCase().equals("SUM") || columnName.toUpperCase().equals("AVG")
                            || columnName.toUpperCase().equals("MIN") || columnName.toUpperCase().equals("MAX")) {
                        columnName = selectColumnNames[i - 1];
                    }

                    int len = rsmd.getColumnDisplaySize(i);
                    int precision = rsmd.getScale(i);
                    int type = Util.convertToSystemDataType(String.valueOf(rsmd.getColumnType(i)), String.valueOf(len), String.valueOf(precision));

                    columnNames.add(columnName);
                    columnInfo = new HashMap<>();

                    columnInfo.put("type", String.valueOf(type));
                    columnInfo.put("description", columnName);
                    columnInfo.put("len", String.valueOf(len));
                    columnInfo.put("precision", String.valueOf(precision));

                    if (needDatasetInfo) {
                        columnInfoMap.put(columnName, columnInfo);
                    }
                }

                datasetInfo = new DatasetInfo();
                dataset.setDatasetInfo(datasetInfo);

                datasetInfo.setDatasetName("");
                datasetInfo.setDatasetSourceType(0);
                datasetInfo.setDatasetSource("");
                datasetInfo.setColumnNames(columnNames);
                datasetInfo.setColumnInfo(columnInfoMap);

                int k = 0;
                boolean hasMore = false;

                do {
                    k++;
                    totalRows++;

                    if (k < startFrom + 1) {
                        resultSet.next();
                        continue;
                    }

                    if (k > maxExpectHits && maxExpectHits > 0) {
                        hasMore = true;
                        break;
                    }

                    Map<String, String> map = new HashMap<>();

                    map.put("rowId", String.valueOf(k));

                    int kk = 1;
                    for (String cName : columnNames) {
                        if (cName.equals("rowId")) {
                            continue;
                        }

                        String columnValue = resultSet.getString(kk);

                        if (columnValue == null) {
                            columnValue = "";
                        }

                        map.put(cName, columnValue);
                        kk++;
                    }

                    dataset.getRows().add(map);
                } while (resultSet.next());

                if (hasMore) {
                    while (resultSet.next()) {
                        totalRows++;
                    }
                }

                dataset.setTotalRow(totalRows);
                dataset.setCurrentRow(dataset.getRows().size());
            }

            resultSet.close();
        }

        return dataset;
    }

    public static DataApiCall saveNewDataAPICallInfo(int organizationId, EntityManager em, int appId, int userId, String apiName, String parameterInfo) {
        em.getTransaction().begin();

        DataApiCall dataApiCall = new DataApiCall();
        dataApiCall.setOrganizationId(organizationId);
        dataApiCall.setAppId(appId);
        dataApiCall.setUserId(userId);
        dataApiCall.setApiName(apiName);
        dataApiCall.setApiCallParameters(parameterInfo);
        dataApiCall.setCallTime(new Date());
        em.persist(dataApiCall);

        em.getTransaction().commit();

        return dataApiCall;
    }

    public static DataApiCall updateDataAPICallInfo(EntityManager em, DataApiCall dataApiCall, String resultStr, String execptionStr, boolean isSuccessful) {
        em.getTransaction().begin();

        dataApiCall.setIsSuccessful(isSuccessful ? (short) 1 : (short) 0);
        dataApiCall.setApiCallReturn(resultStr);
        dataApiCall.setApiCallException(execptionStr);
        dataApiCall.setFinishTime(new Date());

        DataApiCall newDataApiCall = em.merge(dataApiCall);

        em.getTransaction().commit();

        return newDataApiCall;
    }

    public static EntityManagerFactory getRecEntityManagerFactory(DatasourceConnection datasourceConnection) {
        try {
            //log.info(" getPlatformEntityManagerFactory() ...");

            if (recEmf == null) {
                try {
                    Map<String, Object> persistenceMap = new HashMap<>();

                    Map<String, String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());

                    persistenceMap.put("hibernate.connection.url", datasourceConnection.getLocation());
                    persistenceMap.put("hibernate.connection.username", datasourceConnection.getUserName());
                    persistenceMap.put("hibernate.connection.password", datasourceConnection.getPassword());
                    //persistenceMap.put("javax.persistence.lock.timeout", CommonKeys.JPA_LOCK_TIMEOUT); // 1 second

                    log.info(String.format("Connecting TO db %s: url=%s,username=%s,password=%s", CommonKeys.REC_DATABASE_NAME, properties.get("url"), properties.get("username"), properties.get("password")));

                    recEmf = Persistence.createEntityManagerFactory(CommonKeys.REC_DATABASE_NAME, persistenceMap);
                } catch (Exception e) {
                    log.error("getPlatformEntityManagerFactory() error! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e));
                }
            }

            //log.info("exit getPlatformEntityManagerFactory() ...  systemEmf="+systemEmf);
            return recEmf;
        } catch (Exception e) {
            log.error("failed to getPlatformEntityManagerFactory! e=", e);
            return null;
        }
    }

    public static Repository getConfigRepository(EntityManager em, int organizationId) {
        String sql = String.format("from Repository where organizationId=%d and useAsOrganizationRepository=1", organizationId);
        Repository repository = (Repository) em.createQuery(sql).getSingleResult();
        return repository;
    }

    public static long getQueueSize(EntityManager platformEm, int organizationId, String queueName) throws Exception {
        String mqIP = Util.getMQServiceIPs(platformEm, organizationId);
        mqIP = mqIP.substring(0, mqIP.indexOf(":"));

        return JMSUtil.getQueuePendingMessageCount(mqIP, "localhost", queueName);
    }

    public static String getDataobjectTypeTableName1(EntityManager em, EntityManager platformEm, String dataobjectTypeName, int datasourceId, int sqldbId) {
        Datasource datasource = em.find(Datasource.class, datasourceId);

        DatasourceConnection datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());
        String prefix = Util.getDatasourceConnectionProperty(datasourceConnection, "table_name_prefix");

        if (prefix == null) {
            prefix = "";
        }

        return String.format("%s%s", prefix, dataobjectTypeName.trim());
    }

    public static String getDataobjectTableName(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId, String dataobjectTypeName, int repositoryId) {
        //String sourceTablePrefix = Util.getDataobjectTypeDatasourceConnectionPropertyName(em,platformEm,organizationId,dataobjectTypeId,"table_name_prefix");
        //  String targetTablePrefix = Util.getRepositorySQLdbTableNamePrefix(em,platformEm,repositoryId);  
        String tableName = String.format("%s", dataobjectTypeName.trim());

        return tableName;
    }

    public static String getDataobjectTableTablespace(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId, int repositoryId) {
        String sourceTablespace = Util.getDataobjectTypeDatasourceConnectionPropertyName(em, platformEm, organizationId, dataobjectTypeId, "table_space");
        String targetTablespace = getRepositorySQLdbPropertyName(em, platformEm, repositoryId, "table_space");

        if (sourceTablespace != null && !sourceTablespace.isEmpty()) {
            return sourceTablespace;
        } else {
            return targetTablespace;
        }
    }

    public static String getDataobjectTableIndexspace(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId, int repositoryId) {
        String sourceIndexspace = Util.getDataobjectTypeDatasourceConnectionPropertyName(em, platformEm, organizationId, dataobjectTypeId, "index_space");
        String targetIndexspace = getRepositorySQLdbPropertyName(em, platformEm, repositoryId, "index_space");

        if (sourceIndexspace != null && !sourceIndexspace.isEmpty()) {
            return sourceIndexspace;
        } else {
            return targetIndexspace;
        }
    }

    public static String getBusinessDateInfo(int organizationId, EntityManager platformEm) {
        String statusStr;
        String info = "";

        try {
            OrganizationBusinessDateInfo organizationBusinessDateInfo = platformEm.find(OrganizationBusinessDateInfo.class, organizationId);

            if (organizationBusinessDateInfo == null || organizationBusinessDateInfo.getIsEnabled() == 0) {
                return "";
            }

            String dateStr = Tool.convertDateToString(organizationBusinessDateInfo.getCurrentBusinessDate(), "yyyy-MM-dd");

            if (organizationBusinessDateInfo.getStatus() == OrganizationBusinessStatus.IMPORTING_DATA.getValue()) {
                statusStr = getBundleMessage(OrganizationBusinessStatus.IMPORTING_DATA.name());
            } else if (organizationBusinessDateInfo.getStatus() == OrganizationBusinessStatus.FINISH_IMPORTING_DATA.getValue()) {
                statusStr = getBundleMessage(OrganizationBusinessStatus.FINISH_IMPORTING_DATA.name());
            } else {
                statusStr = getBundleMessage("daily_data_batch");
            }

            info = String.format("%s %s,%s", dateStr, statusStr, organizationBusinessDateInfo.getCheckInfo());
        } catch (Exception ee) {
            //log.error(" find organization status failed! e="+ee+" organizationId="+organizationId);
        }

        return info;
    }

    public static long getDataobjectTypeDatawareHouseCount(EntityManager em, EntityManager platformEm, int organizationId, int repositoryId, int dataobjectTypeId, String dataobjectTypeName) {
        long count;

        try {
            //String tableNamePrefix = Util.getRepositorySQLdbTableNamePrefix(em,platformEm,repositoryId);
            //String tableName = tableNamePrefix+dataobjectTypeName;

            //String tableName = Util.getDataobjectTableName(em,platformEm,organizationId,dataobjectTypeId,dataobjectTypeName,repositoryId);
            String tableName = dataobjectTypeName;

            DatasourceConnection datasourceConnection = Util.getRepositorySQLdbDatasourceConnection(em, platformEm, repositoryId);

            count = JDBCUtil.getDatabaseTableCountNumber(tableName, datasourceConnection);
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatawareHouseCount() failed! e=" + e);
            return -1;
        }

        return count;
    }

    public static String getDataobjectTypeDatawareHouseTypeName(EntityManager em, EntityManager platformEm, int organizationId, int repositoryId, String dataobjectTypeName) {
        String name = "";

        try {
            DatasourceConnection datasourceConnection = Util.getRepositorySQLdbDatasourceConnection(em, platformEm, repositoryId);
            name = getDatasourceConnectionProperty(datasourceConnection, "database_type");
            name = DatabaseType.findByValue(Integer.parseInt(name)).getDescription();
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatawareHouseCount() failed! e=" + e);
        }

        return name;
    }

    public static long getDataobjectTypeDatasourceCount(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId) {
        long countNumber;
        int datasourceId = 0;

        try {
            String sql = String.format("from Datasource where organizationId=%d ", organizationId);
            List<Datasource> datasourceList = em.createQuery(sql).getResultList();

            for (Datasource datasource : datasourceList) {
                String xml = datasource.getProperties();
                //log.info("xml="+xml);

                if (!xml.contains("targetDataobjectType")) {
                    continue;
                }

                String str = xml.substring(xml.indexOf("<targetDataobjectType>") + "<targetDataobjectType>".length(), xml.indexOf("</targetDataobjectType>"));
                int targetDataobjectType = Integer.parseInt(str);

                if (targetDataobjectType == dataobjectTypeId) {
                    datasourceId = datasource.getId();
                    break;
                }
            }

            if (datasourceId == 0) {
                return -2;
            }

            Datasource datasource = em.find(Datasource.class, datasourceId);
            DatasourceConnection datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());
            countNumber = JDBCUtil.getDatabaseTableCountNumber(datasource, datasourceConnection);
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatasourceCount() failed! e=" + e);
            return -1;
        }

        return countNumber;
    }

    public static String getDataobjectTypeDatasourceConnectionName(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId) {
        String name = "";
        int datasourceId = 0;

        try {
            String sql = String.format("from Datasource where organizationId=%d ", organizationId);
            List<Datasource> datasourceList = em.createQuery(sql).getResultList();

            for (Datasource datasource : datasourceList) {
                String xml = datasource.getProperties();
                //log.info("xml="+xml);

                if (!xml.contains("targetDataobjectType")) {
                    continue;
                }

                String str = xml.substring(xml.indexOf("<targetDataobjectType>") + "<targetDataobjectType>".length(), xml.indexOf("</targetDataobjectType>"));
                int targetDataobjectType = Integer.parseInt(str);

                if (targetDataobjectType == dataobjectTypeId) {
                    datasourceId = datasource.getId();
                    break;
                }
            }

            if (datasourceId == 0) {
                return name;
            }

            Datasource datasource = em.find(Datasource.class, datasourceId);
            DatasourceConnection datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());
            name = datasourceConnection.getName().trim();
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatasourceConnectionName() failed! e=" + e);
        }

        return name;
    }

    public static DatasourceConnection getDataobjectTypeDatasourceConnection(EntityManager em, int organizationId, int dataobjectTypeId) {
        DatasourceConnection datasourceConnection = null;
        int datasourceId = 0;

        try {
            String sql = String.format("from Datasource where organizationId=%d ", organizationId);
            List<Datasource> datasourceList = em.createQuery(sql).getResultList();

            for (Datasource datasource : datasourceList) {
                String xml = datasource.getProperties();
                //log.info("xml="+xml);

                if (!xml.contains("targetDataobjectType")) {
                    continue;
                }

                String str = xml.substring(xml.indexOf("<targetDataobjectType>") + "<targetDataobjectType>".length(), xml.indexOf("</targetDataobjectType>"));
                int targetDataobjectType = Integer.parseInt(str);

                if (targetDataobjectType == dataobjectTypeId) {
                    datasourceId = datasource.getId();
                    break;
                }
            }

            if (datasourceId == 0) {
                return null;
            }

            Datasource datasource = em.find(Datasource.class, datasourceId);
            datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatasourceConnection() failed! e=" + e);
        }

        return datasourceConnection;
    }

    public static String getDataobjectTypeDatasourceConnectionPropertyName(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId, String propertyName) {
        String name = "";
        int datasourceId = 0;

        try {
            String sql = String.format("from Datasource where organizationId=%d ", organizationId);
            List<Datasource> datasourceList = em.createQuery(sql).getResultList();

            for (Datasource datasource : datasourceList) {
                String xml = datasource.getProperties();
                //log.info("xml="+xml);

                if (!xml.contains("targetDataobjectType")) {
                    continue;
                }

                String str = xml.substring(xml.indexOf("<targetDataobjectType>") + "<targetDataobjectType>".length(), xml.indexOf("</targetDataobjectType>"));
                int targetDataobjectType = Integer.parseInt(str);

                if (targetDataobjectType == dataobjectTypeId) {
                    datasourceId = datasource.getId();
                    break;
                }
            }

            if (datasourceId == 0) {
                return name;
            }

            Datasource datasource = em.find(Datasource.class, datasourceId);
            DatasourceConnection datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());

            name = getDatasourceConnectionProperty(datasourceConnection, propertyName);

            if (name == null || name.equals("null")) {
                name = "";
            }

            name = name.trim();
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatasourceConnectionTablePrefix() failed! e=" + e);
        }

        return name;
    }

    public static String getDataobjectTypeDatasourceTypeName(EntityManager em, EntityManager platformEm, int organizationId, int dataobjectTypeId) {
        String name = "";
        int datasourceId = 0;

        try {
            String sql = String.format("from Datasource where organizationId=%d ", organizationId);
            List<Datasource> datasourceList = em.createQuery(sql).getResultList();

            for (Datasource datasource : datasourceList) {
                String xml = datasource.getProperties();
                //log.info("xml="+xml);

                if (!xml.contains("targetDataobjectType")) {
                    continue;
                }

                String str = xml.substring(xml.indexOf("<targetDataobjectType>") + "<targetDataobjectType>".length(), xml.indexOf("</targetDataobjectType>"));
                int targetDataobjectType = Integer.parseInt(str);

                if (targetDataobjectType == dataobjectTypeId) {
                    datasourceId = datasource.getId();
                    break;
                }
            }

            if (datasourceId == 0) {
                return name;
            }

            Datasource datasource = em.find(Datasource.class, datasourceId);
            DatasourceConnection datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());

            name = getDatasourceConnectionProperty(datasourceConnection, "database_type");
            name = DatabaseType.findByValue(Integer.parseInt(name)).getDescription();
        } catch (Exception e) {
            log.error(" getDataobjectTypeDatasourceCount() failed! e=" + e);
        }

        return name;
    }

    public static List<String> getDataobjectTypeIndexNameList(EntityManager em, DataobjectType dataobjectType, Organization organization, Repository repository) {
        String indexSetName;
        List<String> indexNameList = new ArrayList<>();
        String sql;

        if (dataobjectType.getIsEventData() == 0) {
            indexSetName = String.format("%%_%d_%d_data_event_yearmonth_000000_%%", organization.getId(), repository.getId());
            sql = String.format("from IndexSet where name like '%s' ", indexSetName);

            List<IndexSet> indexSetList = em.createQuery(sql).getResultList();
            for (IndexSet indexSet : indexSetList) {
                indexNameList.add(indexSet.getName());
            }
        } else {     // search this value in Index
            indexSetName = String.format("%%_%d_%d_data_event_yearmonth_%%", organization.getId(), repository.getId());
            sql = String.format("from IndexSet where name like '%s' ", indexSetName);

            List<IndexSet> indexSetList = em.createQuery(sql).getResultList();
            for (IndexSet indexSet : indexSetList) {
                if (indexSet.getName().contains("yearmonth_000000_alias")) {
                    continue;
                }

                indexNameList.add(indexSet.getName());
            }
        }

        return indexNameList;
    }

    public static List<Map<String, String>> getDataviewColumnInfo(String dataviewOutputColumns) {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> map;

        if (dataviewOutputColumns == null || dataviewOutputColumns.trim().isEmpty()) {
            return list;
        }

        String[] vals = dataviewOutputColumns.split("\\;");

        for (String val : vals) {
            String[] nextVals = val.split("\\,");

            try {
                map = new HashMap<>();
                map.put("name", nextVals[0]);
                map.put("description", nextVals[2]);
                map.put("dataType", nextVals[3]);
                map.put("length", nextVals[4]);

                if (nextVals.length == 6) {
                    map.put("precision", nextVals[5]);
                } else {
                    map.put("precision", "");
                }

                list.add(map);
            } catch (Exception e) {
                log.info("1111111111 error outputcolumn =" + val);
            }
        }

        return list;
    }

    public static Map<String, String> generateInsertSql(DatabaseType databaseType, Map<String, String> tableColumnNameMap, String tableColumnNameStr, List<String> primaryKeyNames, Map<String, String> columnLenPrecisionMap, String tableName, List<Map<String, Object>> metadataInfoList, List<FrontendMetadata> metadataList) {
        Map<String, String> sqlList = new HashMap<>();
        String tableColumnName;
        long dateValue;

        String insertSql = String.format("INSERT INTO %s ( %s ) VALUES (", tableName, tableColumnNameStr);
        String updateSql = String.format("UPDATE %s SET ", tableName);
        String whereSql = String.format("WHERE ");

        for (Map<String, Object> definition : metadataInfoList) {
            String columnName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
            int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));

            String value = Util.findSingleValueMetadataValue(columnName, metadataList);

            if (value == null || value.equals("null")) {
                value = "";
            } else {
                value = value.trim();

                value = Tool.removeSpecialChar(value);

                if (!value.isEmpty() && Tool.isNumber(value)) {
                    value = Tool.normalizeNumber(value);
                }
            }

            value = Tool.removeQuotation(value);

            if (dataType == MetadataDataType.DATE.getValue()) {
                //long dateValue = Tool.convertESDateStringToLong(value);
                //Date gmtDate = Tool.changeFromGmt(new Date(dateValue));

                if (value.isEmpty()) {
                    dateValue = 0;
                } else {
                    dateValue = Long.parseLong(value);
                }

                value = Tool.convertDateToDateString(new Date(dateValue));
            } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                //long dateValue = Tool.convertESDateStringToLong(value);
                //Date gmtDate = Tool.changeFromGmt(new Date(dateValue));

                if (value.isEmpty()) {
                    dateValue = 0;
                } else {
                    dateValue = Long.parseLong(value);
                }

                value = Tool.convertDateToTimestampStringWithMilliSecond1(new Date(dateValue));
            }

            if (databaseType == DatabaseType.ORACLE) {
                if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                        || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                        || dataType == MetadataDataType.DOUBLE.getValue()) {
                    if (value.isEmpty()) {
                        String clp = columnLenPrecisionMap.get(columnName);
                        String defaultNullVal = Tool.getDefaultNullVal(clp);

                        insertSql += String.format("'%s',", defaultNullVal);
                    } else {
                        insertSql += String.format("%s,", value);
                    }
                } else if (dataType == MetadataDataType.DATE.getValue()) {
                    insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),", value);
                } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                    insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),", value.substring(0, 19));
                } else {
                    insertSql += String.format("'%s',", value);
                }
            } else if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                    || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                    || dataType == MetadataDataType.DOUBLE.getValue()) {
                if (value.isEmpty()) {
                    String clp = columnLenPrecisionMap.get(columnName);
                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                    insertSql += String.format("'%s',", defaultNullVal);
                } else {
                    insertSql += String.format("%s,", value);
                }
            } else {
                insertSql += String.format("'%s',", value);
            }

            // for updatesql
            boolean isPrimaryKey = false;

            for (String kName : primaryKeyNames) {
                if (kName.equals(columnName)) {
                    isPrimaryKey = true;
                    break;
                }
            }

            //tableColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(),columnName);
            tableColumnName = tableColumnNameMap.get(columnName);

            if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                    || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                    || dataType == MetadataDataType.DOUBLE.getValue()) {
                if (value.isEmpty()) {
                    String clp = columnLenPrecisionMap.get(columnName);
                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                    if (isPrimaryKey) {
                        whereSql += String.format(" %s = '%s' AND", tableColumnName, defaultNullVal);
                    } else {
                        updateSql += String.format(" %s = '%s',", tableColumnName, defaultNullVal);
                    }
                } else if (isPrimaryKey) {
                    whereSql += String.format(" %s = %s AND", tableColumnName, value);
                } else {
                    updateSql += String.format(" %s = %s,", tableColumnName, value);
                }
            } else if (isPrimaryKey) {
                whereSql += String.format(" %s = '%s' AND", tableColumnName, value);
            } else {
                updateSql += String.format(" %s = '%s',", tableColumnName, value);
            }
        }

        insertSql = insertSql.substring(0, insertSql.length() - 1) + ")";

        if (updateSql != null) {
            updateSql = updateSql.substring(0, updateSql.length() - 1);

            if (whereSql != null && !whereSql.trim().isEmpty()) {
                whereSql = whereSql.substring(0, whereSql.length() - 3);
            } else {
                whereSql = "";
            }

            updateSql += " " + whereSql;
        }

        sqlList.put("insertSql", insertSql);
        sqlList.put("updateSql", updateSql);

        return sqlList;
    }

    public static Date getStartTimeByTimeUnit(Date time, int timeUnitId) {
        Date newTime = null;

        if (time == null || timeUnitId == 0) {
            return new Date(0);
        }

        TimeUnit timeUnit = TimeUnit.findByValue(timeUnitId);

        switch (timeUnit) {
            case SECOND:
                newTime = time;
            case MINUTE:
                newTime = Tool.startOfMinute(time);
                break;
            case HOUR:
                newTime = Tool.startOfHour(time);
                break;
            case DAY:
                newTime = Tool.startOfDay(time);
                break;
            case WEEK:
                newTime = Tool.startOfWeek(time);
                break;
            case MONTH:
                newTime = Tool.startOfMonth(time);
                break;
            case YEAR:
                newTime = Tool.startOfYear(time);
                break;
        }

        return newTime;
    }

    public static Date getEndTimeByTimeUnit(Date time, int timeUnitId) {
        Date newTime = null;
        TimeUnit timeUnit = TimeUnit.findByValue(timeUnitId);

        switch (timeUnit) {
            case MINUTE:
                newTime = Tool.endOfMinute(time);
                break;
            case HOUR:
                newTime = Tool.endOfHour(time);
                break;
            case DAY:
                newTime = Tool.endOfDay(time);
                break;
            case WEEK:
                newTime = Tool.endOfWeek(time);
                break;
            case MONTH:
                newTime = Tool.endOfMonth(time);
                break;
            case YEAR:
                newTime = Tool.endOfYear(time);
                break;
        }

        return newTime;
    }

    public static String getBusinessTimeString(Date businessTime, MetricsDefinition metricsDefinition) {
        String str = null;

        TimeUnit timeUnit = TimeUnit.findByValue(metricsDefinition.getTimeUnit());

        switch (timeUnit) {
            case MINUTE:
                str = Tool.getYearMonthDayHourMinuteStr(businessTime);
                break;
            case HOUR:
                str = Tool.getYearMonthDayHourStr(businessTime);
                break;
            case DAY:
                str = Tool.getYearMonthDayStr(businessTime);
                break;
            case WEEK:
                str = Tool.getYearWeekStr(businessTime);
                break;
            case MONTH:
                str = Tool.getYearMonthStr(businessTime);
                break;
            case YEAR:
                str = Tool.getYearStr(businessTime);
                break;
        }

        return str;
    }

    public static List<PlatformMetrics> getPlatformMetrics(EntityManager platformEm, int organizationId, int platformMetricsId) throws Exception {
        List<PlatformMetrics> platformMetricsList = new ArrayList<>();
        String sql;

        try {
            if (platformMetricsId == 0) {
                sql = String.format("from PlatformMetrics where organizationId=%d", organizationId);
            } else {
                sql = String.format("from PlatformMetrics where organizationId=%d and id=%d", organizationId, platformMetricsId);
            }

            platformMetricsList = platformEm.createQuery(sql).getResultList();
        } catch (Exception e) {
            log.error(" getPlatformMetrics() failed! e=" + e);
            throw e;
        }

        return platformMetricsList;
    }

    public static List<Map<String, String>> getPlatformMetricsHistory(EntityManager platformEm, int timeAggregationType, int timeNum, PlatformMetrics platformMetrics) {
        List<Map<String, String>> historyData = new ArrayList<>();
        Map<String, String> data;
        double value;
        double lastValue = 0.0;
        Date currentDate = new Date();

        for (int i = timeNum; i >= 0; i--) {
            data = new HashMap<>();

            if (timeAggregationType == TimeAggregationType.DAY.getValue()) {
                Date date = Tool.dateAddDiff(currentDate, -1 * i * 24, Calendar.HOUR);

                Date date1 = Tool.startOfDay(date);
                String dateStr1 = Tool.convertDateToTimestampString(date1);

                Date date2 = Tool.endOfDay(date);
                String dateStr2 = Tool.convertDateToTimestampString(date2);

                String sql = String.format("select avg(value) from PlatformMetricsHistory where metricsId=%d and metricsTime>='%s'and metricsTime<='%s'", platformMetrics.getId(), dateStr1, dateStr2);
                //log.info("sql="+sql);

                Query query = platformEm.createQuery(sql);

                try {
                    value = (Double) query.getSingleResult(); //log.info(" value="+value);
                } catch (Exception e) {
                    log.error("select from metrics history failed! e=" + e);
                    value = 0.0;
                }

                if (i < timeNum) {
                    data.put("metricsId", String.valueOf(platformMetrics.getId()));
                    data.put("metricsName", platformMetrics.getName());
                    data.put("time", Tool.convertDateToDateString(date1));
                    data.put("currentDataNum", String.valueOf(value));
                    data.put("newDataNum", String.valueOf(value - lastValue));

                    historyData.add(data);
                }

                lastValue = value;
            } else {

            }
        }

        return historyData;
    }

    public static String replaceFilterString(String filterStr, Map<String, String> queryParameters) {
        if (filterStr == null || queryParameters == null || queryParameters.isEmpty()) {
            return "";
        }

        String newVal = filterStr;

        for (Map.Entry<String, String> entry : queryParameters.entrySet()) {
            //log.info(" newVal="+newVal+" input="+entry.getKey()+" replace value="+entry.getValue());
            newVal = newVal.replace(entry.getKey(), entry.getValue());
        }

        return newVal;
    }

    public static String replaceFilterString(String filterStr, List<Map<String, Object>> queryParameters) {
        String cid;
        String operatorName;
        int dataType;
        boolean isSingleValue;
        Object metadataFromValue = null;
        Object metadataToValue = null;
        Object metadataValue = null;
        String endDatetime = "";
        String newVal;

        for (Map<String, Object> parameter : queryParameters) {
            cid = (String) parameter.get("cid");
            dataType = Integer.parseInt((String) parameter.get("dataType"));
            isSingleValue = (Boolean) parameter.get("singleInputMetadataValue");
            operatorName = (String) parameter.get("operatorName");

            if (dataType == MetadataDataType.DATE.getValue()) {
                metadataFromValue = Tool.convertDateToDateStringForES((Date) parameter.get(CommonKeys.CRITERIA_KEY_metadataFromValueDate));
                metadataToValue = Tool.convertDateToDateStringForES((Date) parameter.get(CommonKeys.CRITERIA_KEY_metadataToValueDate));
                metadataValue = Tool.convertDateToDateStringForES((Date) parameter.get(CommonKeys.CRITERIA_KEY_metadataValueDate));

                endDatetime = Tool.getEndDatetime((String) metadataValue);
            } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                metadataFromValue = Tool.convertDateToTimestampStringForES((Date) parameter.get(CommonKeys.CRITERIA_KEY_metadataFromValueDate));
                metadataToValue = Tool.convertDateToTimestampStringForES((Date) parameter.get(CommonKeys.CRITERIA_KEY_metadataToValueDate));
                metadataValue = Tool.convertDateToTimestampStringForES((Date) parameter.get(CommonKeys.CRITERIA_KEY_metadataValueDate));

                endDatetime = Tool.getEndDatetime((String) metadataValue);
            } else {
                metadataFromValue = parameter.get(CommonKeys.CRITERIA_KEY_metadataFromValue);
                metadataToValue = parameter.get(CommonKeys.CRITERIA_KEY_metadataToValue);
                metadataValue = parameter.get(CommonKeys.CRITERIA_KEY_metadataValue);
            }

            newVal = filterStr;
            log.info(" old filter str=" + newVal + " singleValue=" + isSingleValue + " cid=" + cid + " metadatValue=" + metadataValue);

            log.info(" query.getFilterString=" + filterStr);
            if (isSingleValue) {
                if ((dataType == MetadataDataType.TIMESTAMP.getValue() || dataType == MetadataDataType.DATE.getValue()) && operatorName.equals("eq")) {
                    newVal = filterStr.replace(cid, String.format("%s TO %s", (String) metadataValue, endDatetime));
                } else {
                    newVal = filterStr.replace(cid, metadataValue.toString());
                }
            } else {
                newVal = filterStr.replace(cid, String.format("%s TO %s", metadataFromValue, metadataToValue));
            }

            filterStr = newVal;
            log.info(" new filterStr=" + filterStr);
        }

        return filterStr;
    }

    public static String getFilterStr(List<List<Map<String, Object>>> criteria, List<Map<String, Object>> groupCriteriaInfo) {
        int i, j, k = 1;
        Integer[] dataobjectTypeIds;

        String singleStr = null;
        String groupStr = null;
        String filterStr;

        String indexType;
        String fieldName;
        int dataType;
        String operatorName;

        Object metadataFromValue = null;
        Object metadataToValue = null;
        Object timeUnitValue = null;
        Object metadataValue = null;
        String endDatetime = "";
        boolean selectPredefineRelativeTime;
        boolean selectRelativeTime;
        boolean isRuntimeInput;
        int criterionType = 1;

        if (criteria == null) {
            return "";
        }

        i = 0;
        filterStr = "(";

        for (List<Map<String, Object>> groupCriterion : criteria) // AND
        {
            criterionType = 1;

            for (Map<String, Object> obj : groupCriteriaInfo) {
                if (obj.get("selectedGroupCriteria").equals(groupCriterion)) {
                    Object ct = obj.get("criterionType");

                    if (ct != null) {
                        criterionType = (Integer) ct;
                    } else {
                        criterionType = BooleanOperatorType.MUST.getValue();
                    }

                    break;
                }
            }

            j = 0;
            groupStr = "(";

            for (Map<String, Object> singleCriterion : groupCriterion) // OR
            {
                fieldName = (String) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectedMetadata);
                fieldName = fieldName.trim();

                dataType = (Integer) singleCriterion.get(CommonKeys.CRITERIA_KEY_dataType);

                operatorName = (String) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectedOperator);
                dataobjectTypeIds = (Integer[]) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectedTargetDataobjectTypes);

                selectPredefineRelativeTime = singleCriterion.get(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) == null ? false : (Boolean) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime);
                selectRelativeTime = singleCriterion.get(CommonKeys.CRITERIA_KEY_selectRelativeTime) == null ? false : (Boolean) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectRelativeTime);

                isRuntimeInput = singleCriterion.get(CommonKeys.CRITERIA_KEY_isRuntimeInput) == null ? false : (Boolean) singleCriterion.get(CommonKeys.CRITERIA_KEY_isRuntimeInput);

                if (selectRelativeTime) {
                    metadataValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValue);
                    timeUnitValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_timeUnitValue);
                } else if (selectPredefineRelativeTime) {
                    metadataValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValue);  //log.info("metadataValue="+metadataValue);
                } else if (dataType == MetadataDataType.DATE.getValue()) {
                    metadataFromValue = Tool.convertDateToDateStringForES((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataFromValueDate));
                    metadataToValue = Tool.convertDateToDateStringForES((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataToValueDate));
                    metadataValue = Tool.convertDateToDateStringForES((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValueDate));

                    //endDatetime = Tool.getEndDatetime((String)metadataValue);
                } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                    metadataFromValue = Tool.convertDateToTimestampStringForES((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataFromValueDate));
                    metadataToValue = Tool.convertDateToTimestampStringForES((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataToValueDate));
                    metadataValue = Tool.convertDateToTimestampStringForES((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValueDate));

                    //endDatetime = Tool.getEndDatetime((String)metadataValue);
                } else {
                    metadataFromValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataFromValue);
                    metadataToValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataToValue);
                    metadataValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValue);
                }

                indexType = "";

                log.info(" operator name=" + operatorName + " metadataValue=" + metadataValue);

                if (isRuntimeInput == true) {
                    //singleStr = "object_size:[0 TO *]";

                    if (dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()) {
                        switch (operatorName) {
                            case "eq":
                                singleStr = String.format("%s%s:[$(%d)]", indexType, fieldName, k);
                                break;
                            case "ibt":
                                singleStr = String.format("%s%s:[$(%d)]", indexType, fieldName, k);
                                break;
                            case "ebt":
                                singleStr = String.format("%s%s:{$(%d)}", indexType, fieldName, k);
                                break;
                            case "gt":
                                singleStr = String.format("%s%s:{$(%d) TO *]", indexType, fieldName, k);
                                break;
                            case "gte":
                                singleStr = String.format("%s%s:[$(%d) TO *]", indexType, fieldName, k);
                                break;
                            case "lt":
                                singleStr = String.format("%s%s:[* TO $(%d)}", indexType, fieldName, k);
                                break;
                            case "lte":
                                singleStr = String.format("%s%s:[* TO $(%d)]", indexType, fieldName, k);
                                break;
                            case "pdrt":
                                singleStr = String.format("%s%s:[%s-$(%d)]", indexType, fieldName, CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime, k);
                                break;
                            case "rt":
                                singleStr = String.format("%s%s:[%s-$(%d)]", indexType, fieldName, CommonKeys.CRITERIA_KEY_selectRelativeTime, k);
                                break;
                        }
                        k++;
                    } else {
                        switch (operatorName) {
                            case "eq":
                                singleStr = String.format("%s%s:$(%d)", indexType, fieldName, k);
                                break;
                            case "like":
                                singleStr = String.format("%s%s:$(%d)*", indexType, fieldName, k);
                                break;
                            case "ibt":
                                singleStr = String.format("%s%s:[$(%d)]", indexType, fieldName, k);
                                break;
                            case "ebt":
                                singleStr = String.format("%s%s:{$(%d)}", indexType, fieldName, k);
                                break;
                            case "gt":
                                singleStr = String.format("%s%s:>$(%d)", indexType, fieldName, k);
                                break;
                            case "gte":
                                singleStr = String.format("%s%s:>=$(%d)", indexType, fieldName, k);
                                break;
                            case "lt":
                                singleStr = String.format("%s%s:<$(%d)", indexType, fieldName, k);
                                break;
                            case "lte":
                                singleStr = String.format("%s%s:<=$(%d)", indexType, fieldName, k);
                                break;
                        }
                        k++;
                    }
                } else if (dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()) {
                    switch (operatorName) {
                        case "eq":
                            endDatetime = Tool.getEndDatetime((String) metadataValue);
                            singleStr = String.format("%s%s:[%s TO %s]", indexType, fieldName, metadataValue, endDatetime);
                            break;
                        case "ibt":
                            endDatetime = Tool.getEndDatetime((String) metadataToValue);
                            singleStr = String.format("%s%s:[%s TO %s]", indexType, fieldName, metadataFromValue, endDatetime);
                            break;
                        case "ebt":
                            metadataFromValue = Tool.changeMilliSecondTo999((String) metadataFromValue);
                            singleStr = String.format("%s%s:{%s TO %s}", indexType, fieldName, metadataFromValue, metadataToValue);
                            break;
                        case "gt":
                            metadataValue = Tool.changeMilliSecondTo999((String) metadataValue);
                            singleStr = String.format("%s%s:{%s TO *]", indexType, fieldName, metadataValue);
                            break;
                        case "gte":
                            singleStr = String.format("%s%s:[%s TO *]", indexType, fieldName, metadataValue);
                            break;
                        case "lt":
                            metadataValue = Tool.changeMilliSecondTo0((String) metadataValue);
                            singleStr = String.format("%s%s:[* TO %s}", indexType, fieldName, metadataValue);
                            break;
                        case "lte":
                            singleStr = String.format("%s%s:[* TO %s]", indexType, fieldName, metadataValue);
                            break;
                        case "pdrt":
                            singleStr = String.format("%s%s:[%s-%s]", indexType, fieldName, CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime, metadataValue);
                            break;
                        case "rt":
                            singleStr = String.format("%s%s:[%s-%s-%s]", indexType, fieldName, CommonKeys.CRITERIA_KEY_selectRelativeTime, metadataValue, timeUnitValue);
                            break;
                    }
                } else {
                    switch (operatorName) {
                        case "eq":
                            if (metadataValue instanceof String && ((String) metadataValue).trim().isEmpty()) {
                                metadataValue = String.format("\"%s\"", metadataValue);
                            }

                            singleStr = String.format("%s%s:%s", indexType, fieldName, metadataValue);
                            break;
                        case "like":
                            singleStr = String.format("%s%s:/%s/", indexType, fieldName, metadataValue);
                            break;
                        case "ibt":
                            singleStr = String.format("%s%s:[%s TO %s]", indexType, fieldName, metadataFromValue, metadataToValue);
                            break;
                        case "ebt":
                            singleStr = String.format("%s%s:{%s TO %s}", indexType, fieldName, metadataFromValue, metadataToValue);
                            break;
                        case "gt":
                            singleStr = String.format("%s%s:>%s", indexType, fieldName, metadataValue);
                            break;
                        case "gte":
                            singleStr = String.format("%s%s:>=%s", indexType, fieldName, metadataValue);
                            break;
                        case "lt":
                            singleStr = String.format("%s%s:<%s", indexType, fieldName, metadataValue);
                            break;
                        case "lte":
                            singleStr = String.format("%s%s:<=%s", indexType, fieldName, metadataValue);
                            break;
                    }
                }

                if (j == 0) {
                    groupStr = String.format("%s %s", groupStr, singleStr);
                } else {
                    groupStr = String.format("%s OR %s", groupStr, singleStr);
                }

                j++;
            }
            if (j > 0) {
                groupStr = groupStr.concat(")");
            } else {
                groupStr = "";
            }

            if (groupStr.length() == 0) {
                continue;
            }

            log.info("1111 criterionType=" + criterionType);

            if (criterionType == BooleanOperatorType.MUST.getValue()) {
                if (i == 0) {
                    filterStr = String.format("%s %s", filterStr, groupStr);
                } else {
                    filterStr = String.format("%s AND %s", filterStr, groupStr);
                }
            } else if (i == 0) {
                filterStr = String.format("%s NOT %s", filterStr, groupStr);
            } else {
                filterStr = String.format("%s AND NOT %s", filterStr, groupStr);
            }

            i++;
        }

        if (i > 0) {
            filterStr = filterStr.concat(")");
        } else {
            filterStr = "";
        }

        return filterStr;
    }

    public static String getSqlWhereStr(String dataobjectTypeName, List<List<Map<String, Object>>> criteria, List<Map<String, Object>> groupCriteriaInfo) {
        int i, j, k = 1;
        Integer[] dataobjectTypeIds;

        String singleStr = null;
        String groupStr = null;
        String filterStr;

        String fieldName;
        int dataType;
        String operatorName;

        Object metadataFromValue = null;
        Object metadataToValue = null;
        Object timeUnitValue = null;
        Object metadataValue = null;
        String endDatetime = "";
        boolean selectPredefineRelativeTime;
        boolean selectRelativeTime;

        int criterionType = 1;

        if (criteria == null) {
            return "";
        }

        i = 0;
        filterStr = "";

        for (List<Map<String, Object>> groupCriterion : criteria) // AND
        {
            criterionType = 1;

            for (Map<String, Object> obj : groupCriteriaInfo) {
                if (obj.get("selectedGroupCriteria").equals(groupCriterion)) {
                    Object ct = obj.get("criterionType");

                    if (ct != null) {
                        criterionType = (Integer) ct;
                    } else {
                        criterionType = BooleanOperatorType.MUST.getValue();
                    }

                    break;
                }
            }

            j = 0;
            groupStr = "(";

            for (Map<String, Object> singleCriterion : groupCriterion) // OR
            {
                fieldName = (String) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectedMetadata);
                fieldName = fieldName.trim();

                fieldName = Tool.changeTableColumnName(dataobjectTypeName, fieldName, true);

                dataType = (Integer) singleCriterion.get(CommonKeys.CRITERIA_KEY_dataType);

                operatorName = (String) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectedOperator);
                dataobjectTypeIds = (Integer[]) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectedTargetDataobjectTypes);

                selectPredefineRelativeTime = singleCriterion.get(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime) == null ? false : (Boolean) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectPredefineRelativeTime);
                selectRelativeTime = singleCriterion.get(CommonKeys.CRITERIA_KEY_selectRelativeTime) == null ? false : (Boolean) singleCriterion.get(CommonKeys.CRITERIA_KEY_selectRelativeTime);

                if (selectRelativeTime) {
                    metadataValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValue);
                    timeUnitValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_timeUnitValue);
                } else if (selectPredefineRelativeTime) {
                    metadataValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValue);  //log.info("metadataValue="+metadataValue);
                } else if (dataType == MetadataDataType.DATE.getValue()) {
                    metadataFromValue = Tool.convertDateToString((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataFromValueDate), "yyyy-MM-dd");
                    metadataToValue = Tool.convertDateToString((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataToValueDate), "yyyy-MM-dd");
                    metadataValue = Tool.convertDateToString((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValueDate), "yyyy-MM-dd");
                } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                    metadataFromValue = Tool.convertDateToString((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataFromValueDate), "yyyy-MM-dd HH:mm:ss");
                    metadataToValue = Tool.convertDateToString((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataToValueDate), "yyyy-MM-dd HH:mm:ss");
                    metadataValue = Tool.convertDateToString((Date) singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValueDate), "yyyy-MM-dd HH:mm:ss");
                } else {
                    metadataFromValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataFromValue);
                    metadataToValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataToValue);
                    metadataValue = singleCriterion.get(CommonKeys.CRITERIA_KEY_metadataValue);
                }

                log.info(" operator name=" + operatorName + " metadataValue=" + metadataValue);

                if (dataType == MetadataDataType.DATE.getValue() || dataType == MetadataDataType.TIMESTAMP.getValue()) {
                    switch (operatorName) {
                        case "eq":
                            singleStr = String.format("%s='%s'", fieldName, metadataValue);
                            break;
                        case "ibt":
                            singleStr = String.format("%s>='%s' and %s<='%s'", fieldName, metadataFromValue, fieldName, metadataToValue);
                            break;
                        case "ebt":
                            singleStr = String.format("%s>'%s and %s<'%s'", fieldName, metadataFromValue, fieldName, metadataToValue);
                            break;
                        case "gt":
                            singleStr = String.format("%s>'%s'", fieldName, metadataValue);
                            break;
                        case "gte":
                            singleStr = String.format("%s>='%s'", fieldName, metadataValue);
                            break;
                        case "lt":
                            singleStr = String.format("%s<'%s'", fieldName, metadataValue);
                            break;
                        case "lte":
                            singleStr = String.format("%s<='%s'", fieldName, metadataValue);
                            break;
                    }
                } else if (dataType == MetadataDataType.STRING.getValue()) {
                    switch (operatorName) {
                        case "eq":
                            singleStr = String.format("%s='%s'", fieldName, metadataValue);
                            break;
                        case "like":
                            singleStr = String.format("%s like '%%%s%%'", fieldName, metadataValue);
                            break;
                        case "ibt":
                            singleStr = String.format(" %s>='%s' and %s<='%s'", fieldName, metadataFromValue, fieldName, metadataToValue);
                            break;
                        case "ebt":
                            singleStr = String.format(" %s>'%s' and %s<'%s'", fieldName, metadataFromValue, fieldName, metadataToValue);
                            break;
                        case "gt":
                            singleStr = String.format("%s>'%s'", fieldName, metadataValue);
                            break;
                        case "gte":
                            singleStr = String.format("%s>='%s'", fieldName, metadataValue);
                            break;
                        case "lt":
                            singleStr = String.format("%s<'%s'", fieldName, metadataValue);
                            break;
                        case "lte":
                            singleStr = String.format("%s<='%s'", fieldName, metadataValue);
                            break;
                    }
                } else {
                    switch (operatorName) {
                        case "eq":
                            singleStr = String.format("%s=%s", fieldName, metadataValue);
                            break;
                        case "ibt":
                            singleStr = String.format(" %s>=%s and %s<=%s", fieldName, metadataFromValue, fieldName, metadataToValue);
                            break;
                        case "ebt":
                            singleStr = String.format(" %s>%s and %s<%s", fieldName, metadataFromValue, fieldName, metadataToValue);
                            break;
                        case "gt":
                            singleStr = String.format("%s>%s", fieldName, metadataValue);
                            break;
                        case "gte":
                            singleStr = String.format("%s>=%s", fieldName, metadataValue);
                            break;
                        case "lt":
                            singleStr = String.format("%s<%s", fieldName, metadataValue);
                            break;
                        case "lte":
                            singleStr = String.format("%s<=%s", fieldName, metadataValue);
                            break;
                    }
                }

                if (j == 0) {
                    groupStr = String.format("%s %s", groupStr, singleStr);
                } else {
                    groupStr = String.format("%s OR %s", groupStr, singleStr);
                }

                j++;
            }
            if (j > 0) {
                groupStr = groupStr.concat(")");
            } else {
                groupStr = "";
            }

            if (groupStr.length() == 0) {
                continue;
            }

            log.info("1111 criterionType=" + criterionType);

            if (criterionType == BooleanOperatorType.MUST.getValue()) {
                if (i == 0) {
                    filterStr = String.format("%s %s", filterStr, groupStr);
                } else {
                    filterStr = String.format("%s AND %s", filterStr, groupStr);
                }
            } else if (i == 0) {
                filterStr = String.format("%s NOT %s", filterStr, groupStr);
            } else {
                filterStr = String.format("%s AND NOT %s", filterStr, groupStr);
            }

            i++;
        }

        if (i > 0) {
            filterStr = filterStr.concat("");
        } else {
            filterStr = "";
        }

        return filterStr;
    }

    public static List<String> getTaskDatasourceIds(String taskConfig, String nodeName) {
        Document taskConfigXml = Tool.getXmlDocument(taskConfig);

        List<String> datasourceIds = new ArrayList<>();
        List<Element> list = taskConfigXml.selectNodes(nodeName);

        for (Element element : list) {
            datasourceIds.add(element.getText());
        }

        return datasourceIds;
    }

    public static DataobjectTypeAssociation getDataobjectTypeAssociation(EntityManager platformEm, int organizationId, int thisDataobjectType, int associatedDataobjectTypeId) {
        String sql = null;
        DataobjectTypeAssociation dta = null;

        try {
            sql = String.format("from DataobjectTypeAssociation where organizationId=%d and masterDataobjectTypeId=%d and slaveDataobjectTypeId=%d", organizationId, thisDataobjectType, associatedDataobjectTypeId);
            dta = (DataobjectTypeAssociation) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error(" getDataobjectTypeAssociation() failed! e=" + e + " sql=" + sql);
        }

        return dta;

        /*    try
        {
            sql = String.format("from DataobjectTypeAssociation where organizationId=%d and masterDataobjectTypeId=%d and slaveDataobjectTypeId=%d",organizationId,associatedDataobjectTypeId,thisDataobjectType);
            dta = (DataobjectTypeAssociation)platformEm.createQuery(sql).getSingleResult();
            return dta;
        }       
        catch(Exception e)
        {   
        }                
        
        return dta;*/
    }

    public static int getDateTimeDataType(String datetimeFormat) throws Exception {
        //"yyyy-MM-dd HH:mm:ss.SSS";

        if (!datetimeFormat.contains("yyyy") || !datetimeFormat.contains("MM") || !datetimeFormat.contains("dd")) {
            throw new Exception("wrong datetime format! datetimeFormat=" + datetimeFormat);
        }

        if (datetimeFormat.contains("HH") || datetimeFormat.contains("mm") || datetimeFormat.contains("ss")) {
            return MetadataDataType.TIMESTAMP.getValue();
        } else {
            return MetadataDataType.DATE.getValue();
        }
    }

    public static boolean isDatasourceEndJobProcessingEventData(DatasourceEndJob job, List<StructuredFileDefinition> fileTypeList, EntityManager platformEm) {
        List<String> archiveFileList = Util.getConfigData(job.getConfig(), CommonKeys.ARCHIVE_FILE_NODE);
        String filename = FileUtil.getFileNameWithoutPath(archiveFileList.get(0));
        StructuredFileDefinition fileDefinition = (StructuredFileDefinition) getFileDefinition(job.getOrganizationId(), filename, fileTypeList, platformEm);

        return fileDefinition.getIsEventData() == 1;
    }

    public static StructuredFileDefinition getFileDefinition(int organizationId, String filename, List<StructuredFileDefinition> fileTypeList, EntityManager platformEm) {
        String sql;
        Pattern pattern;
        boolean matched;
        //List<StructuredFileDefinition> fileTypeList;

        try {
            //sql = String.format("from StructuredFileDefinition where organizationId = %d",organizationId);
            //fileTypeList =  platformEm.createQuery(sql).getResultList();

            for (StructuredFileDefinition fileType : fileTypeList) {
                pattern = Pattern.compile(fileType.getFileNameMatchingRegularExpression());
                matched = pattern.matcher(filename.trim()).matches();

                if (matched) {
                    return fileType;
                }
            }

            return null;
        } catch (Exception e) {
            log.error("failed to getFileDefinition()! e=" + e.getMessage() + " filename=" + filename);
            throw e;
        }
    }

    public static DatasourceConnection getRepositorySQLdbDatasourceConnection(EntityManager em, EntityManager platformEm, int repositoryId) {
        DatasourceConnection datasourceConnection = null;

        try {
            Repository repository = em.find(Repository.class, repositoryId);

            //log.info(" repository sqldb service instance id="+repository.getRecordStorageServiceInstanceId());
            ServiceInstance serviceInstance = platformEm.find(ServiceInstance.class, repository.getRecordStorageServiceInstanceId());

            Map<String, String> properties = getServiceInstancePropertyConfigMap(serviceInstance.getConfig());
            int datasourceConnectionId = Integer.parseInt(properties.get("datasourceConnectionId"));
            datasourceConnection = em.find(DatasourceConnection.class, datasourceConnectionId);
        } catch (Exception e) {
            log.error("getRepositorySQLdbDatasourceConnection() failed! e=" + e + " repositoryId=" + repositoryId);
            throw e;
        }

        return datasourceConnection;
    }

    public static Map<String, String> getEDFVideoProcessingService(EntityManager platformEm, String serviceInstanceId) {
        Map<String, String> properties = null;

        try {
            String sql = String.format("from ServiceInstance where id=%s", serviceInstanceId);
            ServiceInstance serviceInstance = (ServiceInstance) platformEm.createQuery(sql).getSingleResult();

            properties = getServiceInstancePropertyConfigMap(serviceInstance.getConfig());

            properties.put("ips", serviceInstance.getLocation());
        } catch (Exception e) {
            log.error("getEDFVideoProcessingService() failed! e=" + e);
            throw e;
        }

        return properties;
    }

    public static Map<String, String> getKafkaService(EntityManager platformEm) {
        Map<String, String> properties = null;

        try {
            String sql = String.format("from ServiceInstance where type=%d", PlatformServiceType.KAFKA_SERVICE.getValue());
            ServiceInstance serviceInstance = (ServiceInstance) platformEm.createQuery(sql).getSingleResult();

            properties = getServiceInstancePropertyConfigMap(serviceInstance.getConfig());

            properties.put("ips", serviceInstance.getLocation());
        } catch (Exception e) {
            log.error("getEDFVideoProcessingService() failed! e=" + e);
            throw e;
        }

        return properties;
    }

    public static Map<String, String> getRepositoryRelationshipStorageConnection(EntityManager em, EntityManager platformEm, int repositoryId) {
        Map<String, String> properties = null;

        try {
            Repository repository = em.find(Repository.class, repositoryId);

            log.info(" repository relationship storage service instance id=" + repository.getRelationshipStorageServiceInstanceId());
            ServiceInstance serviceInstance = platformEm.find(ServiceInstance.class, repository.getRelationshipStorageServiceInstanceId());

            properties = getServiceInstancePropertyConfigMap(serviceInstance.getConfig());
        } catch (Exception e) {
            log.error("getRepositoryRelationshipStorageConnection() failed! e=" + e + " repositoryId=" + repositoryId);
            throw e;
        }

        return properties;
    }

    public static String getRepositorySQLdbTableNamePrefix(EntityManager em, EntityManager platformEm, int repositoryId) {
        String tableNamePrefix = "";

        try {
            Repository repository = em.find(Repository.class, repositoryId);

            ServiceInstance serviceInstance = platformEm.find(ServiceInstance.class, repository.getRecordStorageServiceInstanceId());
            Map<String, String> properties = getServiceInstancePropertyConfigMap(serviceInstance.getConfig());

            tableNamePrefix = properties.get("tableNamePrefix") == null ? "" : properties.get("tableNamePrefix").trim();
        } catch (Exception e) {
            log.error("getRepositorySQLdbTableNamePrefix() failed! e=" + e + " repositoryId=" + repositoryId);
        }

        return tableNamePrefix;
    }

    public static String getRepositorySQLdbPropertyName(EntityManager em, EntityManager platformEm, int repositoryId, String propertyName) {
        String propertyValue = "";

        try {
            Repository repository = em.find(Repository.class, repositoryId);

            ServiceInstance serviceInstance = platformEm.find(ServiceInstance.class, repository.getRecordStorageServiceInstanceId());
            Map<String, String> properties = getServiceInstancePropertyConfigMap(serviceInstance.getConfig());

            propertyValue = properties.get(propertyName) == null ? "" : properties.get(propertyName).trim();
        } catch (Exception e) {
            log.error("getRepositorySQLdbPropertyName() failed! e=" + e + " repositoryId=" + repositoryId);
        }

        return propertyValue;
    }

    public static Map<String, String> getDatasourceConnectionProperties(String propertyXml) {
        Map<String, String> properties = new HashMap<>();

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(propertyXml);

            List<Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.DATASOURCE_CONNECTION_PROPERTIES);

            for (Element node : nodes) {
                String name = node.element("name") != null ? node.element("name").getTextTrim() : "";
                String value = node.element("value") != null ? node.element("value").getTextTrim() : "";

                properties.put(name, value);
            }
        } catch (Exception e) {
            log.error(" get getDatasourceConnectionProperties() failed! e=" + e);
            throw e;
        }

        return properties;
    }

    public static List<String> createTableForDataobjectType(boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String dataobjectTypeDescription, String tableName, String tableDescription, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns, Map<String, String> columnLenPrecisionMap, String tableSpace, String indexSpace) throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String createTableSql = null;
        Map<String, Object> map;
        List<String> commentSqlList;

        boolean onlyWithPrimaryKey = false;

        try {
            switch (databaseType) {
                case DB2:
                    map = generateDB2CreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case ORACLE:
                case ORACLE_SERVICE:
                    map = generateOracleCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, tableDescription, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case GREENPLUM:
                    map = generateGreenPlumCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    sqlList.add(String.format("COMMENT ON TABLE %s IS '%s'", tableName, dataobjectTypeDescription));

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case SQLSERVER:
                case SQLSERVER2000:
                    map = generateSQLServerCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case HIVE:
                    createTableSql = generateSparkSQLCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns);
                    sqlList.add(createTableSql);

                    if (needEdfIdField) {
                        String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
                        sqlList.add(commentStr);
                    }

                    break;

                default: // mysql, sqlite
                    createTableSql = generateCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns);
                    sqlList.add(createTableSql);

                    if (needEdfIdField) {
                        String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
                        sqlList.add(commentStr);
                    }

                    break;
            }

            log.info("createTableSql=" + createTableSql);
        } catch (Exception e) {
            log.error(" createTableForDataobjectType() failed! e=" + e + " createTableSql=" + createTableSql);
            throw e;
        }

        return sqlList;
    }

    public static List<String> createTableForDataobjectTypeOnlyWithPrimaryKey(boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String dataobjectTypeDescription, String tableName, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns, Map<String, String> columnLenPrecisionMap, String tableSpace, String indexSpace) throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String createTableSql = null;
        Map<String, Object> map;
        List<String> commentSqlList;

        boolean onlyWithPrimaryKey = true;
        boolean needEdfIdField = false;

        try {
            switch (databaseType) {
                case DB2:
                    map = generateDB2CreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case ORACLE:
                    map = generateOracleCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, "", primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case GREENPLUM:
                    map = generateGreenPlumCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    sqlList.add(String.format("COMMENT ON TABLE %s IS '%s'", tableName, dataobjectTypeDescription));

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case SQLSERVER:
                case SQLSERVER2000:
                    map = generateSQLServerCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns, columnLenPrecisionMap);

                    createTableSql = (String) map.get("createTableSql");
                    sqlList.add(createTableSql);

                    commentSqlList = (List<String>) map.get("commentSqlList");
                    for (String commentSql : commentSqlList) {
                        sqlList.add(commentSql);
                    }

                    break;

                case HIVE:
                    createTableSql = generateSparkSQLCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns);
                    sqlList.add(createTableSql);

                    if (needEdfIdField) {
                        String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
                        sqlList.add(commentStr);
                    }

                    break;

                default: // mysql, sqlite
                    createTableSql = generateCreateTableSql(onlyWithPrimaryKey, needEdfIdField, ifDataobjectTypeWithChangedColumnName, dataobjectTypeName, tableName, primaryKeyNames, databaseType, userName, metadataInfoList, tableColumns);
                    sqlList.add(createTableSql);

                    if (needEdfIdField) {
                        String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
                        sqlList.add(commentStr);
                    }

                    break;
            }

            log.info("createTableSql=" + createTableSql);
        } catch (Exception e) {
            log.error(" createTableForDataobjectType() failed! e=" + e + " createTableSql=" + createTableSql);
            throw e;
        }

        return sqlList;
    }

    private static String generateCreateTableSql(boolean onlyWithPrimaryKey, boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String tableName, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns) {
        String fieldStr;
        String sql = String.format("create table %s ( ", tableName);
        String cName;
        String metadataName;
        String tableColumnName;

        for (String column : tableColumns) {
            if (!column.contains("-")) {
                cName = column;
            } else {
                cName = column.substring(0, column.indexOf("-"));
            }

            fieldStr = "";

            boolean found = false;

            for (Map<String, Object> definition : metadataInfoList) {
                metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

                boolean isPrimaryKey = isPrimaryKey(metadataName, primaryKeyNames);

                if (onlyWithPrimaryKey) {
                    if (!isPrimaryKey) {
                        continue;
                    }
                }

                if (!cName.trim().equals(metadataName.trim())) {
                    continue;
                }

                found = true;

                String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                String sqlType = MetadataDataType.findByValue(dataType).getJdbcType();

                if (sqlType.startsWith("VARCHAR")) {
                    if (lenStr == null || lenStr.trim().isEmpty()) {
                        sqlType = String.format("VARCHAR(%d)", 3000);
                    } else if (Integer.parseInt(lenStr) >= 1000) {
                        sqlType = "TEXT";
                    } else {
                        sqlType = String.format("VARCHAR(%s)", lenStr);
                    }
                }

                tableColumnName = Tool.changeTableColumnName(dataobjectTypeName, metadataName, ifDataobjectTypeWithChangedColumnName);

                fieldStr = String.format("%s %s", tableColumnName, sqlType);

                if (description != null && !description.trim().isEmpty()) {
                    fieldStr = String.format(" %s COMMENT '%s'", fieldStr, description);
                }

                break;
            }

            if (found) {
                sql += fieldStr + ",";
            }
        }

        if (primaryKeyNames.isEmpty()) {
            sql = sql.substring(0, sql.length() - 1);
            sql += " )";
        } else {
            String pk = "";

            for (String name : primaryKeyNames) {
                String newName = Tool.changeTableColumnName(dataobjectTypeName, name, ifDataobjectTypeWithChangedColumnName);
                pk += String.format("%s,", newName);
            }

            //for(String name : primaryKeyNames)
            //    pk += String.format("%s,",name);
            pk = pk.substring(0, pk.length() - 1);

            sql = String.format("%s PRIMARY KEY (%s) )", sql, pk);
        }

        return sql;
    }

    private static String generateSparkSQLCreateTableSql(boolean onlyWithPrimaryKey, boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String tableName, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns) {
        String fieldStr;
        String sql = String.format("create table %s ( ", tableName);
        String cName;
        String metadataName;
        String tableColumnName;

        for (String column : tableColumns) {
            if (!column.contains("-")) {
                cName = column;
            } else {
                cName = column.substring(0, column.indexOf("-"));
            }

            fieldStr = "";

            for (Map<String, Object> definition : metadataInfoList) {
                metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

                boolean isPrimaryKey = isPrimaryKey(metadataName, primaryKeyNames);

                if (onlyWithPrimaryKey) {
                    if (!isPrimaryKey) {
                        continue;
                    }
                }

                if (!cName.trim().equals(metadataName.trim())) {
                    continue;
                }

                String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                String sqlType = MetadataDataType.findByValue(dataType).getJdbcType();

                if (sqlType.startsWith("VARCHAR")) {
                    if (lenStr == null || lenStr.trim().isEmpty()) {
                        sqlType = String.format("VARCHAR(%d)", 3000);
                    } else {
                        sqlType = String.format("VARCHAR(%s)", lenStr);
                    }
                }

                tableColumnName = Tool.changeTableColumnName(dataobjectTypeName, metadataName, ifDataobjectTypeWithChangedColumnName);

                fieldStr = String.format("%s %s", tableColumnName, sqlType);

                if (description != null && !description.trim().isEmpty()) {
                    fieldStr = String.format(" %s COMMENT '%s'", fieldStr, description);
                }
            }

            if (!fieldStr.isEmpty()) {
                sql += fieldStr + ",";
            }
        }

        if (needEdfIdField) {
            fieldStr = String.format("edf_id VARCHAR(40)");
            sql += fieldStr + ",";

            fieldStr = String.format("edf_repository_id INTEGER");
            sql += fieldStr + ",";

            fieldStr = String.format("edf_last_modified_time DATETIME");
            sql += fieldStr + ",";
        }

        sql = sql.substring(0, sql.length() - 1);
        sql += " )";

        return sql;
    }

    private static Map<String, Object> generateDB2CreateTableSql(boolean onlyWithPrimaryKey, boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String tableName, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns, Map<String, String> columnLenPrecisionMap, String tablespace, String indexspace) {
        Map<String, Object> map = new HashMap<>();
        List<String> commentSqlList = new ArrayList<>();

        String cName;
        String fieldStr;
        String metadataName;
        String tableColumnName;

        String createTableSql = String.format("create table %s ( ", tableName);

        for (String column : tableColumns) {
            if (!column.contains("-")) {
                cName = column;
            } else {
                cName = column.substring(0, column.indexOf("-"));
            }

            fieldStr = "";

            for (Map<String, Object> definition : metadataInfoList) {
                metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

                boolean isPrimaryKey = isPrimaryKey(metadataName, primaryKeyNames);

                if (onlyWithPrimaryKey) {
                    if (!isPrimaryKey) {
                        continue;
                    }
                }

                if (!cName.trim().equals(metadataName.trim())) {
                    continue;
                }

                String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                String sqlType = MetadataDataType.findByValue(dataType).getJdbcType();

                if (sqlType.startsWith("VARCHAR") || sqlType.startsWith("CHAR")) {
                    if (lenStr == null || lenStr.trim().isEmpty()) {
                        sqlType = String.format("VARCHAR(%d)", 1000);
                    } else {
                        sqlType = String.format("VARCHAR(%d)", Integer.parseInt(lenStr) * 2);
                    }
                } else if (sqlType.equals("DATETIME")) {
                    sqlType = "TIMESTAMP";
                } else if (sqlType.equals("DOUBLE") || sqlType.equals("REAL")) {
                    String clp = columnLenPrecisionMap.get(metadataName);
                    if (clp == null) {
                        sqlType = String.format("DECIMAL(12,2)", columnLenPrecisionMap.get(metadataName));
                    } else {
                        sqlType = String.format("DECIMAL(%s)", clp);
                    }
                }

                tableColumnName = Tool.changeTableColumnName(dataobjectTypeName, metadataName, ifDataobjectTypeWithChangedColumnName);

                if (isPrimaryKey) {
                    fieldStr = String.format("%s %s NOT NULL", tableColumnName, sqlType);
                } else {
                    fieldStr = String.format("%s %s", tableColumnName, sqlType);
                }

                if (description != null && !description.trim().isEmpty()) {
                    String commentStr = String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, tableColumnName, description);
                    commentSqlList.add(commentStr);
                }
            }

            if (!fieldStr.isEmpty()) {
                createTableSql += fieldStr + ",";
            }
        }

        if (needEdfIdField) {
            fieldStr = String.format("edf_id VARCHAR(40)");
            createTableSql += fieldStr + ",";

            fieldStr = String.format("edf_repository_id INTEGER");
            createTableSql += fieldStr + ",";

            fieldStr = String.format("edf_last_modified_time TIMESTAMP");
            createTableSql += fieldStr + ",";

            String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
            commentSqlList.add(commentStr);
        }

        if (primaryKeyNames.isEmpty()) {
            createTableSql = createTableSql.substring(0, createTableSql.length() - 1);
            createTableSql += " )";
        } else {
            String pk = "";

            for (String name : primaryKeyNames) {
                String newName = Tool.changeTableColumnName(dataobjectTypeName, name, ifDataobjectTypeWithChangedColumnName);
                pk += String.format("%s,", newName);
            }

            pk = pk.substring(0, pk.length() - 1);

            createTableSql = String.format("%s PRIMARY KEY (%s) )", createTableSql, pk);
        }

        if (tablespace != null && !tablespace.trim().isEmpty()) {
            createTableSql = String.format("%s IN %s ", createTableSql, tablespace);
        }

        if (indexspace != null && !indexspace.trim().isEmpty()) {
            createTableSql = String.format("%s INDEX IN %s ", createTableSql, indexspace);
        }

        map.put("createTableSql", createTableSql);
        map.put("commentSqlList", commentSqlList);

        return map;
    }

    private static Map<String, Object> generateOracleCreateTableSql(boolean onlyWithPrimaryKey, boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String tableName, String tableDescription, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns, Map<String, String> columnLenPrecisionMap) {
        Map<String, Object> map = new HashMap<>();
        List<String> commentSqlList = new ArrayList<>();

        String cName;
        String fieldStr;
        String metadataName;
        String tableColumnName;

        tableName = tableName.toUpperCase();

        String createTableSql = String.format("create table %s ( ", tableName);

        if (tableDescription != null && !tableDescription.trim().isEmpty()) {
            String commentStr = String.format("COMMENT ON TABLE %s IS '%s'", tableName, tableDescription);
            commentSqlList.add(commentStr);
        }

        for (String column : tableColumns) {
            if (!column.contains("-")) {
                cName = column;
            } else {
                cName = column.substring(0, column.indexOf("-"));
            }

            fieldStr = "";

            for (Map<String, Object> definition : metadataInfoList) {
                metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

                boolean isPrimaryKey = isPrimaryKey(metadataName, primaryKeyNames);

                if (onlyWithPrimaryKey) {
                    if (!isPrimaryKey) {
                        continue;
                    }
                }

                if (!cName.trim().equals(metadataName.trim())) {
                    continue;
                }

                String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                String sqlType = MetadataDataType.findByValue(dataType).getJdbcType();

                if (sqlType.startsWith("VARCHAR") || sqlType.startsWith("CHAR")) {
                    if (lenStr == null || lenStr.trim().isEmpty()) {
                        sqlType = String.format("VARCHAR2(%d)", 1000);
                    } else {
                        int len = Integer.parseInt(lenStr);

                        len = len * 2;

                        if (len > 4000 || len < 0) {
                            sqlType = String.format("CLOB");
                        } else {
                            sqlType = String.format("VARCHAR2(%d)", len);
                        }
                        //sqlType = String.format("VARCHAR(%d)", Integer.parseInt(lenStr)*2);
                    }
                } else if (sqlType.equals("DATETIME")) {
                    sqlType = "TIMESTAMP";
                } else if (sqlType.equals("DOUBLE") || sqlType.equals("REAL")) {
                    String clp = columnLenPrecisionMap.get(metadataName);
                    if (clp == null) {
                        sqlType = String.format("DECIMAL(12,2)", columnLenPrecisionMap.get(metadataName));
                    } else {
                        sqlType = String.format("DECIMAL(%s)", clp);
                    }
                } else if (sqlType.equals("BIGINT")) {
                    sqlType = "NUMBER";
                } else if (sqlType.equals("TINYINT")) {
                    sqlType = "SMALLINT";
                }

                tableColumnName = Tool.changeTableColumnName(dataobjectTypeName, metadataName, ifDataobjectTypeWithChangedColumnName);

                if (isPrimaryKey) {
                    fieldStr = String.format("%s %s NOT NULL", tableColumnName, sqlType);
                } else {
                    fieldStr = String.format("%s %s", tableColumnName, sqlType);
                }

                if (description != null && !description.trim().isEmpty()) {
                    String commentStr = String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, tableColumnName, description);
                    commentSqlList.add(commentStr);
                }
            }

            if (!fieldStr.isEmpty()) {
                createTableSql += fieldStr + ",";
            }
        }

        if (needEdfIdField) {
            fieldStr = String.format("edf_id VARCHAR(40)");
            createTableSql += fieldStr + ",";

            fieldStr = String.format("edf_repository_id INTEGER");
            createTableSql += fieldStr + ",";

            fieldStr = String.format("edf_last_modified_time DATE");
            createTableSql += fieldStr + ",";

            String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
            commentSqlList.add(commentStr);
        }

        if (primaryKeyNames.isEmpty()) {
            createTableSql = createTableSql.substring(0, createTableSql.length() - 1);
            createTableSql += " )";
        } else {
            String pk = "";

            for (String name : primaryKeyNames) {
                String newName = Tool.changeTableColumnName(dataobjectTypeName, name, ifDataobjectTypeWithChangedColumnName);
                pk += String.format("%s,", newName);
            }

            pk = pk.substring(0, pk.length() - 1);

            createTableSql = String.format("%s PRIMARY KEY (%s) )", createTableSql, pk);
        }

        map.put("createTableSql", createTableSql);
        map.put("commentSqlList", commentSqlList);

        return map;
    }

    private static Map<String, Object> generateGreenPlumCreateTableSql(boolean onlyWithPrimaryKey, boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String tableName, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns, Map<String, String> columnLenPrecisionMap, String tablespace, String indexspace) {
        Map<String, Object> map = new HashMap<>();
        List<String> commentSqlList = new ArrayList<>();

        String cName;
        String fieldStr;
        String metadataName;
        String tableColumnName;

        String createTableSql = String.format("create table %s ( ", tableName);

        for (String column : tableColumns) {
            if (!column.contains("-")) {
                cName = column;
            } else {
                cName = column.substring(0, column.indexOf("-"));
            }

            fieldStr = "";

            for (Map<String, Object> definition : metadataInfoList) {
                metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

                boolean isPrimaryKey = isPrimaryKey(metadataName, primaryKeyNames);

                if (onlyWithPrimaryKey) {
                    if (!isPrimaryKey) {
                        continue;
                    }
                }

                if (!cName.trim().equals(metadataName.trim())) {
                    continue;
                }

                String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                String sqlType = MetadataDataType.findByValue(dataType).getJdbcType();
                String precisionStr = (String) definition.get(CommonKeys.METADATA_NODE_PRECISION);

                log.info("name=" + metadataName + " precision=" + precisionStr + " sqltype=" + sqlType + " lenStr=" + lenStr);

                if (sqlType.startsWith("VARCHAR") || sqlType.startsWith("CHAR")) {
                    if (lenStr == null || lenStr.trim().isEmpty()) {
                        sqlType = String.format("VARCHAR(%d)", 1000);
                    } else if (Integer.parseInt(lenStr) >= 1000) {
                        sqlType = "TEXT";
                    } else {
                        sqlType = String.format("VARCHAR(%d)", Integer.parseInt(lenStr));
                    }
                } else if (sqlType.equals("DATETIME")) {
                    sqlType = "TIMESTAMP";
                } else if (sqlType.equals("DOUBLE") || sqlType.equals("REAL")) {
                    String clp = columnLenPrecisionMap.get(metadataName);
                    if (clp == null || clp.equals("0,0") || clp.equals("0") || clp.trim().isEmpty()) {
                        sqlType = String.format("DECIMAL");
                    } else {
                        sqlType = String.format("DECIMAL(%s)", clp);
                    }
                } else if (sqlType.equals("INTEGER")) {
                    int precision;

                    if (precisionStr == null || precisionStr.trim().isEmpty()) {
                        precision = 0;
                    } else {
                        precision = Integer.parseInt(precisionStr);
                    }

                    log.info("name=" + metadataName + " precision=" + precision + " sqltype=" + sqlType);

                    if (precision <= 0) {
                        sqlType = String.format("DECIMAL(24,4)");
                    }
                } else if (sqlType.equals("TINYINT")) {
                    sqlType = "INTEGER";
                }

                tableColumnName = Tool.changeTableColumnName(dataobjectTypeName, metadataName, ifDataobjectTypeWithChangedColumnName);

                if (JDBCUtil.isGPReservedWord(tableColumnName)) {
                    tableColumnName = tableColumnName.trim() + "_edf";
                }

                if (isPrimaryKey) {
                    fieldStr = String.format("%s %s NOT NULL", tableColumnName, sqlType);
                } else {
                    fieldStr = String.format("%s %s", tableColumnName, sqlType);
                }

                if (description != null && !description.trim().isEmpty()) {
                    String commentStr = String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, tableColumnName, description);
                    commentSqlList.add(commentStr);
                }
            }

            if (!fieldStr.isEmpty()) {
                createTableSql += fieldStr + ",";
            }
        }

        // edf_id
        /*  if ( needEdfIdField )
        {
            fieldStr = String.format("edf_id VARCHAR(40)");
            createTableSql += fieldStr + ","; 
            
            fieldStr = String.format("edf_repository_id INTEGER");
            createTableSql += fieldStr + ","; 
            
            fieldStr = String.format("edf_last_modified_time TIMESTAMP");
            createTableSql += fieldStr + ","; 

            String commentStr = String.format("CREATE INDEX edf_id_index ON %s (edf_id)",tableName);
            commentSqlList.add(commentStr);
        }*/
        if (primaryKeyNames.isEmpty()) {
            createTableSql = createTableSql.substring(0, createTableSql.length() - 1);
            createTableSql += " )";
        } else {
            String pk = "";

            for (String name : primaryKeyNames) {
                String newName = Tool.changeTableColumnName(dataobjectTypeName, name, ifDataobjectTypeWithChangedColumnName);
                pk += String.format("%s,", newName);
            }

            pk = pk.substring(0, pk.length() - 1);

            createTableSql = String.format("%s PRIMARY KEY (%s) )", createTableSql, pk);
        }

        if (tablespace != null && !tablespace.trim().isEmpty()) {
            createTableSql = String.format("%s TABLESPACE %s ", createTableSql, tablespace);
        }

        // if ( indexspace != null && !indexspace.trim().isEmpty() )
        //     createTableSql = String.format("%s INDEXSPACE %s ",createTableSql,indexspace);
        map.put("createTableSql", createTableSql);
        map.put("commentSqlList", commentSqlList);

        return map;
    }

    public static List<String> generateGreenPlumCreateTableSqlFromConnMetadata(Connection dbConn, String columnSchema, boolean needEdfIdField, boolean needEdfLastModifyTimeField, String tableName, String targetTableName, DatabaseType databaseType, String tablespace, String indexspace, String targetUser) throws SQLException, Exception {
        List<String> sqlList = new ArrayList<>();
        List<String> commentSqlList = new ArrayList<>();

        Map<String, Object> columnInfo;
        List<String> primaryKeyColumnNames = new ArrayList<>();
        String sqlType;
        String fieldStr;
        String lenStr;
        String precisionStr;
        boolean isPrimaryKey;

        DatabaseMetaData databaseMetaData = dbConn.getMetaData();

        ResultSet primaryKeyRs = databaseMetaData.getPrimaryKeys(null, null, tableName);

        while (primaryKeyRs.next()) {
            String primaryKeyColumnName = primaryKeyRs.getString("COLUMN_NAME");
            primaryKeyColumnNames.add(primaryKeyColumnName);
        }

        String createTableSql = String.format("create table %s ( \n", targetTableName);

        ResultSet columnSet = databaseMetaData.getColumns(null, null, tableName, "%");

        while (columnSet.next()) {
            columnInfo = new HashMap<>();

            columnInfo.put("isPrimaryKey", false);

            String tableSchema = columnSet.getString("TABLE_SCHEM");
            String columnName = columnSet.getString("COLUMN_NAME");

            if (columnSchema != null && !columnSchema.isEmpty() && !columnSchema.equals("null")) {
                if (tableSchema == null) {
                    continue;
                }

                if (!tableSchema.toUpperCase().equals(columnSchema.toUpperCase())) {
                    continue;
                }
            }

            columnInfo.put("columnName", columnName);
            columnInfo.put("jdbcDataType", columnSet.getString("DATA_TYPE"));
            columnInfo.put("typeName", columnSet.getString("TYPE_NAME"));

            String description = Tool.removeSpecificCharForNameAndDescription(columnSet.getString("REMARKS"));
            columnInfo.put("description", description);

            columnInfo.put("length", columnSet.getString("COLUMN_SIZE"));
            columnInfo.put("precision", columnSet.getString("DECIMAL_DIGITS"));

            int dataType = Util.convertToSystemDataType((String) columnInfo.get("jdbcDataType"), (String) columnInfo.get("length"), (String) columnInfo.get("precision"));
            sqlType = MetadataDataType.findByValue(dataType).getJdbcType();

            lenStr = columnSet.getString("COLUMN_SIZE");
            precisionStr = columnSet.getString("DECIMAL_DIGITS");

            if (sqlType.startsWith("VARCHAR") || sqlType.startsWith("CHAR")) {
                if (lenStr == null || lenStr.trim().isEmpty()) {
                    sqlType = String.format("VARCHAR(%d)", 1000);
                } else if (Integer.parseInt(lenStr) > 1000) {
                    sqlType = "TEXT";
                } else {
                    sqlType = String.format("VARCHAR(%d)", Integer.parseInt(lenStr));
                }
            } else if (sqlType.equals("DATETIME")) {
                sqlType = "TIMESTAMP";
            } else if (sqlType.equals("DOUBLE") || sqlType.equals("REAL")) {
                sqlType = String.format("DECIMAL");
            } else if (sqlType.equals("INTEGER")) {
                int precision;

                if (precisionStr == null || precisionStr.trim().isEmpty()) {
                    precision = 0;
                } else {
                    precision = Integer.parseInt(precisionStr);
                }

                if (precision <= 0) {
                    sqlType = String.format("DECIMAL(24,4)");
                }
            } else if (sqlType.equals("BLOB")) {
                sqlType = String.format("BYTEA");
            }

            isPrimaryKey = false;

            for (String pColumnName : primaryKeyColumnNames) {
                if (pColumnName.equals(columnName)) {
                    isPrimaryKey = true;
                    break;
                }
            }

            if (isPrimaryKey) {
                fieldStr = String.format("%s %s NOT NULL", columnName, sqlType);
            } else {
                fieldStr = String.format("%s %s", columnName, sqlType);
            }

            if (description != null && !description.trim().isEmpty()) {
                String commentStr = String.format("COMMENT ON COLUMN %s.%s IS '%s';\n", targetTableName, columnName, description);
                commentSqlList.add(commentStr);
            }

            createTableSql += "  " + fieldStr + ",\n";
        }

        createTableSql = createTableSql.substring(0, createTableSql.length() - 2); // remove ,\n

        if (needEdfIdField) {
            fieldStr = String.format(",\n  edf_id VARCHAR(40)");
            createTableSql += fieldStr;
        }

        if (needEdfLastModifyTimeField) {
            fieldStr = String.format(",\n  edf_last_modified_time TIMESTAMP");
            createTableSql += fieldStr;
        }

        if (primaryKeyColumnNames.isEmpty()) {
            createTableSql += " )";
        } else {
            String pk = "";

            for (String name : primaryKeyColumnNames) {
                pk += String.format("%s,", name);
            }

            pk = pk.substring(0, pk.length() - 1);

            createTableSql = String.format("%s,\n  PRIMARY KEY (%s) )", createTableSql, pk);
        }

        if (tablespace != null && !tablespace.trim().isEmpty()) {
            createTableSql = String.format("%s TABLESPACE %s", createTableSql, tablespace);
        }

        if (indexspace != null && !indexspace.trim().isEmpty()) {
            createTableSql = String.format("%s INDEXSPACE %s ", createTableSql, indexspace);
        }

        createTableSql += ";\n\n";

        sqlList.add(createTableSql);

        String grantOwnerSql = String.format("ALTER TABLE %s OWNER TO %s;\n", targetTableName, targetUser);
        sqlList.add(grantOwnerSql);

        sqlList.addAll(commentSqlList);

        return sqlList;
    }

    public static Map<String, Object> getTableInfoFromDB(Connection dbConn, String columnSchema, String tableName) throws SQLException, Exception {
        Map<String, Object> tableInfoMap = new HashMap<>();

        Map<String, Object> columnInfo;
        List<String> primaryKeyColumnNames = new ArrayList<>();
        List<Map<String, Object>> metadataInfoList = new ArrayList<>();
        List<String> tableColumns = new ArrayList<>();
        Map<String, String> columnLenPrecisionMap = new HashMap<>();

        String lenStr;
        String precisionStr;

        DatabaseMetaData databaseMetaData = dbConn.getMetaData();

        ResultSet primaryKeyRs = databaseMetaData.getPrimaryKeys(null, null, tableName);

        while (primaryKeyRs.next()) {
            String primaryKeyColumnName = primaryKeyRs.getString("COLUMN_NAME");
            primaryKeyColumnNames.add(primaryKeyColumnName);
        }

        ResultSet columnSet = databaseMetaData.getColumns(null, null, tableName, "%");

        while (columnSet.next()) {
            columnInfo = new HashMap<>();

            columnInfo.put("isPrimaryKey", false);

            String tableSchema = columnSet.getString("TABLE_SCHEM");
            String columnName = columnSet.getString("COLUMN_NAME");

            if (columnSchema != null && !columnSchema.isEmpty() && !columnSchema.equals("null")) {
                if (tableSchema == null) {
                    continue;
                }

                if (!tableSchema.toUpperCase().equals(columnSchema.toUpperCase())) {
                    continue;
                }
            }

            columnInfo.put("name", columnName);
            columnInfo.put("jdbcDataType", columnSet.getString("DATA_TYPE"));
            tableColumns.add(columnName);

            String description = Tool.removeSpecificCharForNameAndDescription(columnSet.getString("REMARKS"));
            columnInfo.put("description", description);

            columnInfo.put("length", columnSet.getString("COLUMN_SIZE"));
            columnInfo.put("precision", columnSet.getString("DECIMAL_DIGITS"));

            int dataType = Util.convertToSystemDataType((String) columnInfo.get("jdbcDataType"), (String) columnInfo.get("length"), (String) columnInfo.get("precision"));
            columnInfo.put("dataType", String.valueOf(dataType));

            lenStr = columnSet.getString("COLUMN_SIZE");
            precisionStr = columnSet.getString("DECIMAL_DIGITS");

            metadataInfoList.add(columnInfo);

            if (lenStr != null && !lenStr.trim().isEmpty()) {
                if (precisionStr == null || precisionStr.trim().isEmpty()) {
                    columnLenPrecisionMap.put(columnName, String.format("%s", lenStr));
                } else {
                    columnLenPrecisionMap.put(columnName, String.format("%s,%s", lenStr, precisionStr));
                }
            }
        }

        tableInfoMap.put("primaryKeyColumnNames", primaryKeyColumnNames);
        tableInfoMap.put("metadataInfoList", metadataInfoList);
        tableInfoMap.put("tableColumns", tableColumns);
        tableInfoMap.put("columnLenPrecisionMap", columnLenPrecisionMap);

        return tableInfoMap;
    }

    private static Map<String, Object> generateSQLServerCreateTableSql(boolean onlyWithPrimaryKey, boolean needEdfIdField, boolean ifDataobjectTypeWithChangedColumnName, String dataobjectTypeName, String tableName, List<String> primaryKeyNames, DatabaseType databaseType, String userName, List<Map<String, Object>> metadataInfoList, List<String> tableColumns, Map<String, String> columnLenPrecisionMap) {
        Map<String, Object> map = new HashMap<>();
        List<String> commentSqlList = new ArrayList<>();

        String cName;
        String fieldStr;
        String metadataName;
        String tableColumnName;

        String createTableSql = String.format("create table %s ( ", tableName);

        for (String column : tableColumns) {
            if (!column.contains("-")) {
                cName = column;
            } else {
                cName = column.substring(0, column.indexOf("-"));
            }

            fieldStr = "";

            for (Map<String, Object> definition : metadataInfoList) {
                metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

                boolean isPrimaryKey = isPrimaryKey(metadataName, primaryKeyNames);

                if (onlyWithPrimaryKey) {
                    if (!isPrimaryKey) {
                        continue;
                    }
                }

                if (!cName.trim().equals(metadataName.trim())) {
                    continue;
                }

                String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                int dataType = Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
                String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                String sqlType = MetadataDataType.findByValue(dataType).getJdbcType();

                if (sqlType.startsWith("VARCHAR") || sqlType.startsWith("CHAR")) {
                    if (lenStr == null || lenStr.trim().isEmpty()) {
                        sqlType = String.format("VARCHAR(%d)", 1000);
                    } else {
                        sqlType = String.format("VARCHAR(%d)", Integer.parseInt(lenStr) * 2);
                    }
                } else if (sqlType.equals("DATE")) {
                    sqlType = "SMALLDATETIME";
                } else if (sqlType.equals("DOUBLE") || sqlType.equals("REAL")) {
                    String clp = columnLenPrecisionMap.get(metadataName);
                    if (clp == null || clp.trim().isEmpty()) {
                        sqlType = String.format("DECIMAL(12,2)", columnLenPrecisionMap.get(metadataName));
                    } else {
                        sqlType = String.format("DECIMAL(%s)", clp);
                    }
                }

                tableColumnName = Tool.changeTableColumnName(dataobjectTypeName, metadataName, ifDataobjectTypeWithChangedColumnName);

                if (isPrimaryKey) {
                    fieldStr = String.format("%s %s NOT NULL", tableColumnName, sqlType);
                } else {
                    fieldStr = String.format("%s %s", tableColumnName, sqlType);
                }

                if (description != null && !description.trim().isEmpty()) {
                    String commentStr = String.format("EXECUTE sp_addextendedproperty N'MS_Description', N'%s', N'user', N'dbo', N'table', N'%s', N'column', N'%s'", description, tableName, tableColumnName);
                    //String commentStr = String.format("COMMENT ON COLUMN %s.%s IS '%s'",tableName,tableColumnName,description);
                    commentSqlList.add(commentStr);
                }
            }

            if (!fieldStr.isEmpty()) {
                createTableSql += fieldStr + ",";
            }
        }

        if (needEdfIdField) {
            fieldStr = String.format("edf_id VARCHAR(40)");
            createTableSql += fieldStr + ",";

            fieldStr = String.format("edf_repository_id INTEGER");
            createTableSql += fieldStr + ",";

            fieldStr = String.format("edf_last_modified_time TIMESTAMP");
            createTableSql += fieldStr + ",";

            String commentStr = String.format("CREATE UNIQUE INDEX edf_id_index ON %s (edf_id)", tableName);
            commentSqlList.add(commentStr);
        }

        if (primaryKeyNames.isEmpty()) {
            createTableSql = createTableSql.substring(0, createTableSql.length() - 1);
            createTableSql += " )";
        } else {
            String pk = "";

            for (String name : primaryKeyNames) {
                String newName = Tool.changeTableColumnName(dataobjectTypeName, name, ifDataobjectTypeWithChangedColumnName);
                pk += String.format("%s,", newName);
            }

            pk = pk.substring(0, pk.length() - 1);

            createTableSql = String.format("%s PRIMARY KEY (%s) )", createTableSql, pk);
        }

        //commentSqlList = new ArrayList<>(); // change later
        map.put("createTableSql", createTableSql);
        map.put("commentSqlList", commentSqlList);

        return map;
    }

    private static boolean isPrimaryKey(String column, List<String> primaryKeyColumns) {
        for (String keyColumnName : primaryKeyColumns) {
            if (keyColumnName.equals(column)) {
                return true;
            }
        }

        return false;
    }

    public static List<String> getAllTargetIndexType(EntityManager platformEm, Integer[] targetDataobjectTypes) {
        List<String> indexTypes = new ArrayList<>();

        if (targetDataobjectTypes == null || targetDataobjectTypes.length == 0) // if no target query data object type selected, choose all types
        {
            return indexTypes;
        }

        Set<Integer> dataobjectTypeIds = new HashSet<>();

        dataobjectTypeIds.addAll(Arrays.asList(targetDataobjectTypes));

        for (Integer dataobjectTypeId : dataobjectTypeIds) {
            String indexType = Util.getIndexType(platformEm, dataobjectTypeId);
            if (indexType.length() > 0) {
                indexTypes.add(indexType);
            }
        }

        return indexTypes;
    }

    public static String getIndexType(EntityManager platformEm, int dataobjectTypeId) {
        String indexType = "";

        try {
            //EntityManager em = getPlatformEntityManagerFactory().createEntityManager();

            String sql = String.format("select d.indexTypeName from DataobjectType d where d.id = %d", dataobjectTypeId);
            indexType = (String) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error("failed to getIndexType e=" + e);
            return "";
        }

        return indexType;
    }

    public static List<DataobjectVO> convertDatasetToDataobjectVOs(Dataset dataset) {
        List<DataobjectVO> dataobjectVOs = new ArrayList<>();
        DataobjectVO dataobjectVO;
        Dataobject dataobject;

        if (dataset == null || dataset.totalRow == 0) {
            return dataobjectVOs;
        }

        for (Map<String, String> row : dataset.getRows()) {
            try {
                dataobject = new Dataobject();
                dataobjectVO = new DataobjectVO();

                String id = row.get("id");
                dataobjectVO.setId(id.substring(0, 40));
                dataobjectVO.setDataobject(dataobject);

                dataobject.setId(id.substring(0, 40)); // dataobject id
                dataobject.setCurrentVersion(Integer.parseInt(id.substring(40))); // dataobject version

                dataobject.setOrganizationId(Integer.parseInt(row.get("organization_id")));
                dataobject.setDataobjectType(Integer.parseInt(row.get("dataobject_type")));
                dataobject.setRepositoryId(Integer.parseInt(row.get("target_repository_id")));

                dataobjectVO.setThisDataobjectTypeMetadatas(row);

                dataobjectVOs.add(dataobjectVO);
            } catch (Exception e) {
                log.error("convertDatasetToDataobjectVOs() failed!  e= " + e);
                throw e;
            }
        }

        return dataobjectVOs;
    }

    public static List<Map<String, Object>> getMetadataInfo(String metadatas) {
        List<Map<String, Object>> metadataInfoList = new ArrayList<>();

        Document metadataXmlDoc = Tool.getXmlDocument(metadatas);

        List<org.dom4j.Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);

        for (org.dom4j.Element node : nodes) {
            Map<String, Object> map = new HashMap<>();

            String metadataName = node.element(CommonKeys.METADATA_NODE_NAME).getTextTrim();
            String description = node.element(CommonKeys.METADATA_NODE_DESCRIPTION) == null ? metadataName : node.element(CommonKeys.METADATA_NODE_DESCRIPTION).getTextTrim();
            String dataType = node.element(CommonKeys.METADATA_NODE_DATA_TYPE) == null ? "" : node.element(CommonKeys.METADATA_NODE_DATA_TYPE).getTextTrim();
            String indexType = node.element(CommonKeys.METADATA_NODE_INDEX_TYPE) == null ? "" : node.element(CommonKeys.METADATA_NODE_INDEX_TYPE).getTextTrim();
            String selectFrom = node.element(CommonKeys.METADATA_NODE_SELECT_FROM) == null ? "" : node.element(CommonKeys.METADATA_NODE_SELECT_FROM).getTextTrim();
            String length = node.element(CommonKeys.METADATA_NODE_LENGTH) == null ? "" : node.element(CommonKeys.METADATA_NODE_LENGTH).getTextTrim();
            String inSearchResult = node.element(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT) == null ? "" : node.element(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT).getTextTrim();
            String flags = node.element(CommonKeys.METADATA_NODE_FLAGS) == null ? "" : node.element(CommonKeys.METADATA_NODE_FLAGS).getTextTrim();

            map.put(CommonKeys.METADATA_NODE_NAME, metadataName);
            map.put(CommonKeys.METADATA_NODE_DESCRIPTION, description);
            map.put(CommonKeys.METADATA_NODE_DATA_TYPE, dataType);
            map.put(CommonKeys.METADATA_NODE_SELECT_FROM, selectFrom);
            map.put(CommonKeys.METADATA_NODE_LENGTH, length);
            map.put(CommonKeys.METADATA_NODE_INDEX_TYPE, indexType);
            map.put(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT, inSearchResult);
            map.put(CommonKeys.METADATA_NODE_FLAGS, flags);

            metadataInfoList.add(map);
        }

        return metadataInfoList;
    }

    // columnInfo = String.format("%s,%s,%s,%s,%s", columnName,columnDescription,dataType,length,"");
    public static List<Map<String, Object>> getDataviewMetadataInfo(String outputColumns) {
        List<Map<String, Object>> metadataInfoList = new ArrayList<>();

        String[] vals = outputColumns.split("\\;");

        for (String columnInfo : vals) {
            String[] column = columnInfo.split("\\,");

            Map<String, Object> map = new HashMap<>();

            map.put(CommonKeys.METADATA_NODE_NAME, column[1]);
            map.put(CommonKeys.METADATA_NODE_DESCRIPTION, column[2]);
            map.put(CommonKeys.METADATA_NODE_DATA_TYPE, column[3]);
            map.put(CommonKeys.METADATA_NODE_LENGTH, column[4]);

            metadataInfoList.add(map);
        }

        return metadataInfoList;
    }

    public static DataobjectType getDataobjectTypeByIndexTypeName(EntityManager platformEm, int organizationId, String index_type) {
        DataobjectType dataobjectType;

        String sql = String.format("from DataobjectType dt where dt.organizationId=%d and dt.indexTypeName='%s'", organizationId, index_type);

        try {
            dataobjectType = (DataobjectType) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            sql = String.format("from DataobjectType dt where dt.organizationId=0 and dt.indexTypeName='%s'", index_type);
            dataobjectType = (DataobjectType) platformEm.createQuery(sql).getSingleResult();
        }

        return dataobjectType;
    }

    public static String getAssociationQueryDataStr(List<Map<String, Object>> associationQueryData) {
        String str;
        StringBuilder builder = new StringBuilder();

        for (Map<String, Object> queryData : associationQueryData) {
            int parentChildSearchType = (Integer) queryData.get("parentChildSearchType");
            int existingTimes = (Integer) queryData.get("existingTimes");
            int childTypeQueryId = queryData.get("childTypeQueryId") == null ? 0 : (Integer) queryData.get("childTypeQueryId");

            if (parentChildSearchType != ParentChildSearchType.NOT_CHECK_IN_CHILD.getValue()) {
                int associatedDataobjectType = (Integer) queryData.get("associatedDataobjectType");
                String associationSearchKeyword = (String) queryData.get("searchKeyword");

                if (associationSearchKeyword == null || associationSearchKeyword.equals("null") || associationSearchKeyword.trim().isEmpty()) {
                    associationSearchKeyword = "";
                }

                str = String.format("%d`%s`%d`%d`%d", associatedDataobjectType, associationSearchKeyword, existingTimes, childTypeQueryId, parentChildSearchType);
                builder.append(str).append("~");
            }
        }

        str = builder.toString();

        if (str.length() > 0) {
            str = str.substring(0, str.length() - 1);
        }

        return str;
    }

    public static List<Map<String, Object>> getAssociationQueryData(String associationQueryDataStr) {
        List<Map<String, Object>> list = new ArrayList<>();

        String[] queryDataList = associationQueryDataStr.split("\\~");

        for (String queryDataStr : queryDataList) {
            String[] fields = queryDataStr.split("\\`");

            Map<String, Object> queryData = new HashMap<>();

            queryData.put("id", UUID.randomUUID().toString());
            queryData.put("associatedDataobjectType", Integer.parseInt(fields[0]));
            queryData.put("searchKeyword", fields[1]);
            queryData.put("parentChildSearchType", Integer.parseInt(fields[4]));
            queryData.put("existingTimes", Integer.parseInt(fields[2]));
            queryData.put("childTypeQueryId", Integer.parseInt(fields[3]));

            queryData.put("associatedDataobjectTypeName", "");
            queryData.put("childTypeQuerySelection", new ArrayList<>());

            list.add(queryData);
        }

        return list;
    }

    public static long getDataobjectTypeCount(int organizationId, EntityManager em, EntityManager platformEm, int repositoryId, String dataobjectTypeIndexTypeName, String queryStr) {
        Client esClient;
        long totalCount = 0;
        List<String> indexNameList = new ArrayList<>();

        try {
            esClient = ESUtil.getClient(em, platformEm, repositoryId, false);

            String sql = String.format("from IndexSet where catalogId in ( select id from Catalog where repositoryId=%d ) ", repositoryId);
            List<IndexSet> indexSetList = em.createQuery(sql).getResultList();

            for (IndexSet indexSet : indexSetList) {
                indexNameList.add(indexSet.getName().trim());
            }

            Map<String, Object> searchRet = ESUtil.retrieveDataset(esClient, indexNameList.toArray(new String[0]), new String[]{dataobjectTypeIndexTypeName}, queryStr, "", 1, 0, new String[]{}, new String[]{}, new String[]{}, null);

            totalCount = (Long) searchRet.get("totalHits");
        } catch (Exception e) {
            if (e instanceof NoNodeAvailableException) {
                ESUtil.getClient(em, platformEm, repositoryId, true);
            }

            return -1;
        }

        return totalCount;
    }

    public static Map<String, Object> getDataobjectTypeESInfo(int organizationId, EntityManager em, EntityManager platformEm, int repositoryId, String dataobjectTypeIndexTypeName) {
        long totalCount = 0;
        String sql = "";
        Map<String, Object> map = new HashMap<>();

        try {
            sql = String.format("from PlatformMetrics where organizationId=%d and computing_method like '%%getCountValueFromES,%d,%s%%'", organizationId, repositoryId, dataobjectTypeIndexTypeName);
            List<PlatformMetrics> list = platformEm.createQuery(sql).getResultList();

            if (list.isEmpty()) {
                map.put("count", (long) 0);
                map.put("dataUpdateInfo", "");
                return map;
            }

            totalCount = list.get(0).getValue().toBigInteger().longValue();

            map.put("count", totalCount);
            map.put("dataUpdateInfo", list.get(0).getOtherInfo());
        } catch (Exception e) {
            log.error(" getDataobjectTypeCountInMetrics() failed! e=" + e + " sql=" + sql);
        }

        return map;
    }

    public static Map<String, Object> getDataobjectTypeDWInfo(int organizationId, EntityManager em, EntityManager platformEm, int repositoryId, int dataobjectTypeId, String dataobjectTypeName) {
        long totalCount = 0;
        String sql = "";
        Map<String, Object> map = new HashMap<>();

        try {
            sql = String.format("from PlatformMetrics where organizationId=%d and computing_method like '%%getCountValueFromDW,%d,%d,%s%%'", organizationId, repositoryId, dataobjectTypeId, dataobjectTypeName);
            List<PlatformMetrics> list = platformEm.createQuery(sql).getResultList();

            if (list.isEmpty()) {
                map.put("count", (long) 0);
                map.put("dataUpdateInfo", "");
                return map;
            }

            totalCount = list.get(0).getValue().toBigInteger().longValue();

            map.put("count", totalCount);
            map.put("dataUpdateInfo", list.get(0).getOtherInfo());
        } catch (Exception e) {
            log.error(" getDataobjectTypeCountInMetrics() failed! e=" + e + " sql=" + sql);
        }

        return map;
    }

    public static Map<String, Object> getDataobjectTypeSourceInfo(int organizationId, EntityManager em, EntityManager platformEm, int repositoryId, int dataobjectTypeId, String dataobjectTypeName) {
        long totalCount = 0;
        String sql = "";
        Map<String, Object> map = new HashMap<>();

        try {
            sql = String.format("from PlatformMetrics where organizationId=%d and computing_method like '%%getCountValueFromSource,%d,%d,%s%%'", organizationId, repositoryId, dataobjectTypeId, dataobjectTypeName);
            List<PlatformMetrics> list = platformEm.createQuery(sql).getResultList();

            if (list.isEmpty()) {
                map.put("count", (long) 0);
                map.put("dataUpdateInfo", "");
                return map;
            }

            totalCount = list.get(0).getValue().toBigInteger().longValue();

            map.put("count", totalCount);
            map.put("dataUpdateInfo", list.get(0).getOtherInfo());
        } catch (Exception e) {
            log.error(" getDataobjectTypeCountInMetrics() failed! e=" + e + " sql=" + sql);
        }

        return map;
    }

    public static String getDataobjectTypeOtherInfo(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) {
        String otherInfo = "";
        String lastStoredTimeStr = "";
        String eventTimeFieldName = "";
        String eventTimeStr = "";
        Date date;

        String repositoryIdStr = parameters[1];
        String indexType = parameters[2].trim();
        //String[] selectedSortFields = new String[]{"last_stored_time,ASCEND"};

        Client esClient = ESUtil.getClient(em, platformEm, Integer.valueOf(repositoryIdStr), false);

        try {
            String[] selectedSortFields = new String[]{"last_stored_time,DESEND"};
            Map<String, Object> ret = ESUtil.searchDocument(esClient, new String[]{}, new String[]{indexType}, new String[]{}, null, null, 1, 0, new String[]{}, selectedSortFields);
            List<Map<String, Object>> searchResults = (List<Map<String, Object>>) ret.get("searchResults");

            if (searchResults == null || searchResults.isEmpty()) {
                return "";
            }

            String jsonStr = (String) searchResults.get(0).get("fields");
            JSONObject json = new JSONObject(jsonStr);

            lastStoredTimeStr = json.optString("last_stored_time");
            date = Tool.convertESDateStringToDate(lastStoredTimeStr);
            lastStoredTimeStr = Tool.convertDateToString(date, "yyyy-MM-dd HH:mm:ss");

            DataobjectType datobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, indexType);

            if (datobjectType.getEventTimeMetadataName() != null && !datobjectType.getEventTimeMetadataName().trim().isEmpty()) {
                List<String> list = Util.getDataobjectTypeFieldNames(platformEm, datobjectType.getId(), true, true);

                for (String name : list) {
                    if (name.startsWith(datobjectType.getEventTimeMetadataName())) {
                        eventTimeFieldName = name;
                        break;
                    }
                }

                eventTimeFieldName = eventTimeFieldName + "(" + getBundleMessage("last") + ")";

                selectedSortFields = new String[]{datobjectType.getEventTimeMetadataName() + ",DESEND"};
                ret = ESUtil.searchDocument(esClient, new String[]{}, new String[]{indexType}, new String[]{}, null, null, 1, 0, new String[]{}, selectedSortFields);
                searchResults = (List<Map<String, Object>>) ret.get("searchResults");
                jsonStr = (String) searchResults.get(0).get("fields");
                json = new JSONObject(jsonStr);

                eventTimeStr = json.optString(datobjectType.getEventTimeMetadataName());
                date = Tool.convertESDateStringToDate(eventTimeStr);
                eventTimeStr = Tool.convertDateToString(date, "yyyy-MM-dd HH:mm:ss");
            }
        } catch (Exception e) {
            log.error(" getDataobjectTypeOtherInfo() failed! e=" + e + ",stacktrace=" + ExceptionUtils.getStackTrace(e));
        }

        if (eventTimeFieldName.isEmpty()) {
            otherInfo = String.format("last_stored_time-%s:%s", getBundleMessage("last_stored_time"), lastStoredTimeStr, eventTimeFieldName, eventTimeStr);
        } else {
            otherInfo = String.format("last_stored_time-%s:%s,%s:%s", getBundleMessage("last_stored_time"), lastStoredTimeStr, eventTimeFieldName, eventTimeStr);
        }

        return otherInfo;
    }

    public static double getCountValueFromES(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) throws Exception {
        Client esClient;
        double count;
        List<String> repositoryList = new ArrayList<>();
        String sql;
        long docCount = 0;

        String repositoryIdStr = parameters[1];
        String indexType = parameters[2].trim();

        if (repositoryIdStr.equals("*")) {
            sql = "from Repository where organizationId=" + organizationId;
            List<Repository> list = em.createQuery(sql).getResultList();

            for (Repository rep : list) {
                repositoryList.add(String.valueOf(rep.getId()));
            }
        } else {
            repositoryList.add(repositoryIdStr);
        }

        for (String repId : repositoryList) {
            esClient = ESUtil.getClient(em, platformEm, Integer.valueOf(repId), false);

            sql = String.format("from IndexSet where catalogId in ( select id from Catalog where repositoryId=%s ) ", repId);
            List<IndexSet> indexSetList = em.createQuery(sql).getResultList();

            for (IndexSet indexSet : indexSetList) {
                try {
                    List<Map<String, String>> result = ESUtil.retrieveIndexTypeInfo(esClient, indexSet.getName());

                    for (Map<String, String> map : result) {
                        String type = (String) map.get("_type");

                        if (indexType.equals("*")) {
                            docCount += Long.parseLong(map.get("count(*)"));
                            continue;
                        }

                        if (type.equals(indexType)) {
                            docCount += Long.parseLong(map.get("count(*)"));
                            break;
                        }
                    }
                } catch (Exception e) {
                    if (e instanceof NoNodeAvailableException) {
                        esClient = ESUtil.getClient(em, platformEm, Integer.valueOf(repId), true);
                    }

                    throw e;
                }
            }
        }

        count = docCount;

        return count;
    }

    public static double getCountValueFromDW(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) throws Exception {
        double count;
        long docCount = 0;

        int repositoryId = Integer.parseInt(parameters[1]);
        int dataobjectTypeId = Integer.parseInt(parameters[2]);
        String dataobjectTypeName = parameters[3];

        docCount = Util.getDataobjectTypeDatawareHouseCount(em, platformEm, organizationId, repositoryId, dataobjectTypeId, dataobjectTypeName.trim());

        count = docCount;

        return count;
    }

    public static double getCountValueFromSource(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) throws Exception {
        double count;
        long docCount = 0;

        int repositoryId = Integer.parseInt(parameters[1]);
        int dataobjectTypeId = Integer.parseInt(parameters[2]);
        String dataobjectTypeName = parameters[3];

        count = Util.getDataobjectTypeDatasourceCount(em, platformEm, organizationId, dataobjectTypeId);

        count = docCount;

        return count;
    }

    public static double getSingleDataFromES(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) throws Exception {
        double val = 0.0;
        String filter = "";
        String sql = "";

        int repositoryId = Integer.parseInt(parameters[1]);
        String indexType = parameters[2].trim();
        String selectColumns = parameters[3];

        if (parameters.length > 4) {
            filter = parameters[4];
        }

        Repository repository = em.find(Repository.class, repositoryId);

        if (filter.isEmpty()) {
            sql = String.format("select %s from %s", selectColumns, indexType);
        } else {
            sql = String.format("select %s from %s where %s", selectColumns, indexType, filter);
        }

        SearchRequest request = SearchUtil.convertSQLLikeStringToRequest(organizationId, repositoryId, repository.getDefaultCatalogId(), sql, 0, 1);
        SearchResponse response = SearchUtil.getDataobjects(em, platformEm, request);
        Dataset dataset = SearchUtil.convertSearchResponseToDataset(platformEm, organizationId, request, response, false);

        for (Map<String, String> row : dataset.getRows()) {
            String valStr = row.get(selectColumns);
            val = Double.parseDouble(valStr);
            break;
        }

        return val;
    }

    public static String getMultipleDataFromES(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) throws Exception {
        String filter = "";
        String sql = "";
        String resultStr = "";

        int repositoryId = Integer.parseInt(parameters[1]);

        Repository repository = em.find(Repository.class, repositoryId);

        String[] vals = parameters[2].split("\\~");

        for (String val : vals) {
            String[] val1 = val.split("\\;");

            String indexType = val1[0];
            String selectColumns = val1[1];

            if (val1.length > 2) {
                filter = val1[2];
            }

            if (filter.isEmpty()) {
                sql = String.format("select %s from %s", selectColumns, indexType);
            } else {
                sql = String.format("select %s from %s where %s", selectColumns, indexType, filter);
            }

            SearchRequest request = SearchUtil.convertSQLLikeStringToRequest(organizationId, repositoryId, repository.getDefaultCatalogId(), sql, 0, 1);
            SearchResponse response = SearchUtil.getDataobjects(em, platformEm, request);
            Dataset dataset = SearchUtil.convertSearchResponseToDataset(platformEm, organizationId, request, response, false);

            for (Map<String, String> row : dataset.getRows()) {
                String valStr = row.get(selectColumns);
                resultStr += valStr + ",";
                break;
            }
        }

        resultStr = Tool.removeLastChars(resultStr, 1);

        return resultStr;
    }

    public static Map<String, String> getDataFromES(int organizationId, EntityManager em, EntityManager platformEm, String[] parameters) throws Exception {
        String filter = "";

        Map<String, String> result = new HashMap<>();
        int repositoryId = Integer.parseInt(parameters[1]);
        String indexType = parameters[2].trim();
        String selectColumns = parameters[3];

        if (parameters.length > 4) {
            filter = parameters[4];
        }

        Repository repository = em.find(Repository.class, repositoryId);

        // sql = "select CORE_BDFMHQAC_AC17CNCD,count(*),sum(CORE_BDFMHQAC_AC10AMT) from CORE_BDFMHQAC_type where CORE_BDFMHQAC_AC10AMT:>2000 group by CORE_BDFMHQAC_AC17CNCD";
        //     dataset = dataService.getESDataBySQL(organizationId, repositoryId, catalogId, sql, searchFrom, maxExpectedHits, needDatasetInfo);
        String sql = String.format("select %s from %s_type where %s", selectColumns, indexType, filter);

        SearchRequest request = SearchUtil.convertSQLLikeStringToRequest(organizationId, repositoryId, repository.getDefaultCatalogId(), sql, 0, 1);
        SearchResponse response = SearchUtil.getDataobjects(em, platformEm, request);
        Dataset dataset = SearchUtil.convertSearchResponseToDataset(platformEm, organizationId, request, response, false);

        for (Map<String, String> row : dataset.getRows()) {
            String[] columns = selectColumns.split("\\~");

            for (String column : columns) {
                result.put(column, row.get(column));
            }
        }

        return result;
    }

    public static String getDataobjectTypeColumns(EntityManager platformEm, int dataobjectTypeId, String selectedColumns, AssociationTypeProperty associationTypeProperty, DataobjectTypeAssociation dta) throws Exception {
        StringBuilder builder = new StringBuilder();

        List<String> columnList = Util.getDataobjectTypeFieldNames(platformEm, dataobjectTypeId, true, true);

        if (associationTypeProperty == AssociationTypeProperty.PROFILE_ASSOCIATION) {
            for (String column : columnList) {
                String col = column.substring(0, column.indexOf("-"));

                if (selectedColumns == null || selectedColumns.trim().isEmpty() || selectedColumns.contains(col)) {
                    builder.append(column).append(";");
                }
            }
        } else {
            String timeColumn = dta.getSlaveTimeEventColumn().trim();

            for (String column : columnList) {
                String col = column.substring(0, column.indexOf("-"));

                if (col.equals(timeColumn)) {
                    builder.append(column).append(";");
                    break;
                }
            }

            for (String column : columnList) {
                String col = column.substring(0, column.indexOf("-"));

                if (col.equals(timeColumn)) {
                    continue;
                }

                if (selectedColumns == null || selectedColumns.trim().isEmpty() || selectedColumns.contains(col)) {
                    builder.append(column).append(";");
                }
            }
        }

        String str = builder.toString();
        return str.substring(0, str.length() - 1);
    }

    public static void storeTagDataInES(EntityManager em, EntityManager platformEm, int organizationId, int repositoryId, List<MetricsTagData> metricsTagDataList, int catalogId, String entityTypeShortName) throws Exception {
        List<String> dataobjectIds;

        try {
            Client esClient = ESUtil.getClient(em, platformEm, repositoryId, false);

            for (MetricsTagData metricsTagData : metricsTagDataList) {
                dataobjectIds = new ArrayList<>();
                dataobjectIds.add(metricsTagData.getEntityId());

                if (metricsTagData.getIsTagValue() == 0) {
                    continue;
                }

                if (metricsTagData.getEntityId().length() != 40) {
                    continue;
                }

                TaggingUtil.addTagToDataobjects(esClient, dataobjectIds, metricsTagData.getMetricsOrTagShortName(), metricsTagData.getValue(), metricsTagData.getValueDescription(), "");
            }
        } catch (Exception e) {
            log.error(" storeTagDataInES() failed! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    public static Map<String, String> getEntityColumnValues(String entityId, EntityManager em, EntityManager platformEm, int organizationId, int entityTypeDataobjectTypeId, int repositoryId) throws Exception {
        Map<String, String> entityColumnValues;
        String dataobjectId = entityId.substring(0, 40);

        JSONObject jsonObject = ESUtil.getDocument(em, platformEm, organizationId, dataobjectId, 1, entityTypeDataobjectTypeId, repositoryId);

        entityColumnValues = Tool.parseJson(jsonObject);

        return entityColumnValues;
    }

    public static void storeMetricsTagDataInES(EntityManager em, EntityManager platformEm, int organizationId, int repositoryId, List<MetricsTagData> metricsTagDataList, int catalogId, String metricsDataobjectTypeName, String entityTypeShortName, Map<String, Object> primaryKeyValues) throws Exception {
        Map<String, Map<String, Object>> data = new HashMap<>();
        Date lastComputingTime = new Date();

        try {
            Client esClient = ESUtil.getClient(em, platformEm, repositoryId, false);

            String entityName = metricsTagDataList.get(0).getEntityName();

            for (MetricsTagData metricsTagData : metricsTagDataList) {
                long businessTimeLong = metricsTagData.getBusinessTime() == null ? 0 : metricsTagData.getBusinessTime().getTime();
                String key = String.format("%s.%d.%s.%d.%d", organizationId, metricsTagData.getEntityType(), metricsTagData.getEntityId(), metricsTagData.getTimeUnit(), businessTimeLong);

                Map<String, Object> metricsData = data.get(key);

                if (metricsData == null) {
                    metricsData = new HashMap<>();
                    data.put(key, metricsData);
                }

                if (metricsTagData.getIsTagValue() == 1) {
                    metricsData.put(metricsTagData.getMetricsOrTagShortName(), metricsTagData.getValue());
                } else {
                    metricsData.put(metricsTagData.getMetricsOrTagShortName(), Double.parseDouble(metricsTagData.getValue()));
                }
            }

            DataobjectType dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, metricsDataobjectTypeName + "_type");

            for (Map.Entry<String, Map<String, Object>> metricsData : data.entrySet()) {
                String[] vals = metricsData.getKey().split("\\.");
                int timeUnit = Integer.parseInt(vals[3]);
                Date businessTime = new Date(Long.parseLong(vals[4]));

                String documentId = DataIdentifier.generateHash(metricsData.getKey());

                String indexName = getTargetIndexName(organizationId, repositoryId, esClient, em, catalogId, businessTime, metricsDataobjectTypeName + "_type", documentId);

                log.debug("11111111111111 indexName=" + indexName);

                Map<String, Object> jsonMap = ESUtil.getRecord(esClient, indexName, metricsDataobjectTypeName + "_type", documentId);

                if (jsonMap == null) {
                    jsonMap = new HashMap<>();
                    jsonMap.put("dataobject_type", dataobjectType.getId());
                    jsonMap.put("target_repository_id", repositoryId);
                    jsonMap.put("event_time", businessTime);
                    jsonMap.put("dataobject_name", metricsData.getKey());
                    jsonMap.put("organization_id", organizationId);
                    jsonMap.put("entity_type", Integer.parseInt(vals[1]));
                    jsonMap.put("entity_id", vals[2]);
                    jsonMap.put("entity_name", entityName);
                    jsonMap.put("business_time", businessTime);
                    jsonMap.put("time_unit", timeUnit);
                }

                jsonMap.put("object_status", DataobjectStatus.UNSYNCHRONIZED.getValue());    /////////////////////
                jsonMap.put("object_status_time", new Date());
                jsonMap.put("last_computing_time", lastComputingTime);
                jsonMap.put("last_stored_time", new Date());

                for (Map.Entry<String, Object> values : primaryKeyValues.entrySet()) {
                    jsonMap.put((metricsDataobjectTypeName + "_" + values.getKey()).toUpperCase(), values.getValue());
                }

                Map<String, Object> metricsValue = metricsData.getValue();

                for (Map.Entry<String, Object> values : metricsValue.entrySet()) {
                    jsonMap.put((metricsDataobjectTypeName + "_" + values.getKey()).toUpperCase(), values.getValue());
                }

                int retry = 0;

                while (true) {
                    retry++;

                    try {
                        ESUtil.persistRecord(esClient, indexName, metricsDataobjectTypeName + "_type", documentId, jsonMap, 0);
                        break;
                    } catch (Exception ee) {
                        if (retry > 10) {
                            throw ee;
                        }

                        log.info(" persiste record failed! e=" + ee + " stacktrace=" + ExceptionUtils.getStackTrace(ee));

                        if (ee instanceof TypeMissingException || ee instanceof StrictDynamicMappingException) {
                            dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, metricsDataobjectTypeName + "_type"); // get new dataobjectType

                            XContentBuilder typeMapping = ESUtil.createTypeMapping(platformEm, dataobjectType, true);
                            ESUtil.putMappingToIndex(esClient, indexName, metricsDataobjectTypeName + "_type", typeMapping);
                            continue;
                        }

                        throw ee;
                    }
                }
            }
        } catch (Exception e) {
            log.error(" storeMetricsTagData() failed! e=" + e);
            throw e;
        }
    }

    public static void storeMetricsTagDataInSqldb(EntityManager em, EntityManager platformEm, int organizationId, int repositoryId, List<MetricsTagData> metricsTagDataList, String metricsDataobjectTypeName, String entityTypeShortName, Map<String, Object> primaryKeyValues, DatasourceConnection datasourceConnection, Connection dbConn, String entityId, String entityName) throws Exception {
        Map<String, Map<String, Object>> data = new HashMap<>();
        List<Map<String, String>> sqldbRows = new ArrayList<>();

        try {
            for (MetricsTagData metricsTagData : metricsTagDataList) {
                long businessTimeLong = metricsTagData.getBusinessTime() == null ? 0 : metricsTagData.getBusinessTime().getTime();
                String key = String.format("%s.%d.%s.%d.%d", organizationId, metricsTagData.getEntityType(), metricsTagData.getEntityKey(), metricsTagData.getTimeUnit(), businessTimeLong);

                Map<String, Object> metricsData = data.get(key);

                if (metricsData == null) {
                    metricsData = new HashMap<>();
                    data.put(key, metricsData);
                }

                if (metricsTagData.getIsTagValue() == 1) {
                    metricsData.put(metricsTagData.getMetricsOrTagShortName(), metricsTagData.getValue());
                } else {
                    metricsData.put(metricsTagData.getMetricsOrTagShortName(), Double.parseDouble(metricsTagData.getValue()));
                }
            }

            for (Map.Entry<String, Map<String, Object>> metricsData : data.entrySet()) {
                String[] vals = metricsData.getKey().split("\\.");
                int timeUnit = Integer.parseInt(vals[3]);
                long businessTimeLong = Long.parseLong(vals[4]);

                Map<String, String> row = new HashMap<>();

                for (Map.Entry<String, Object> values : primaryKeyValues.entrySet()) {
                    row.put((values.getKey()).toUpperCase(), values.getValue().toString());
                }

                row.put("time_unit", String.valueOf(timeUnit));
                row.put("business_time", String.valueOf(businessTimeLong));
                row.put("last_computing_time", String.valueOf(new Date().getTime()));
                row.put("entity_id", entityId);
                row.put("entity_name", entityName);

                Map<String, Object> metricsValue = metricsData.getValue();

                for (Map.Entry<String, Object> values : metricsValue.entrySet()) {
                    row.put((values.getKey()).toUpperCase(), values.getValue().toString());
                }

                sqldbRows.add(row);
            }

            DataobjectType dataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, metricsDataobjectTypeName + "_type");
            List<Map<String, Object>> metadataDefinitions = Util.getDataobjectTypeMetadataDefinition(platformEm, dataobjectType.getId(), true, true);

            DataobjectType parentDataobjectType = Util.getDataobjectTypeByIndexTypeName(platformEm, "metrics_type");
            List<String> parentDataobjectPrimaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(parentDataobjectType.getMetadatas());

            Util.writeDataToSQLDBWithPreparedStatmentFromSQLDB(false, dbConn, metadataDefinitions, dataobjectType.getName(), datasourceConnection,
                    organizationId, dataobjectType, sqldbRows, false, parentDataobjectPrimaryKeyNames);
        } catch (Exception e) {
            log.error(" storeMetricsTagData() failed! e=" + e);
            throw e;
        }
    }

    public static String getTargetIndexName(int organizationId, int repositoryId, Client esClient, EntityManager em, int catalogId, Date businessTime, String indexType, String documentId) throws Exception {
        IndexSet indexSet = null;
        String sql = null;
        String indexName;
        String indexSetName;
        String businessTimeStr = null;

        Catalog catalog = em.find(Catalog.class, catalogId);

        if (catalog == null) {
            log.error(" catalog id not exists! ");
        }

        if (businessTime == null) {
            if (catalog.getPartitionMetadataName().equals("data_event_yearmonth")) {
                businessTimeStr = "000000";
            } else if (catalog.getPartitionMetadataName().equals("data_event_yearmonthday")) {
                businessTimeStr = "00000000";
            } else if (catalog.getPartitionMetadataName().equals("data_event_year")) {
                businessTimeStr = "0000";
            } else {
                businessTimeStr = "0000";
            }

            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        } else if (catalog.getPartitionMetadataName().equals("data_event_yearmonth")) {
            businessTimeStr = Tool.getYearMonthStr(businessTime);
            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        } else if (catalog.getPartitionMetadataName().equals("data_event_yearmonthday")) {
            businessTimeStr = Tool.getYearMonthDayStr(businessTime);
            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        } else if (catalog.getPartitionMetadataName().equals("data_event_year")) {
            businessTimeStr = Tool.getYearStr(businessTime);
            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        }

        try {
            while (true) {
                List<IndexSet> indexSetList = em.createQuery(sql).getResultList();

                if (indexSetList.isEmpty()) {
                    // create new index in search engine
                    indexSetName = Util.getDataMartIndexSetName(organizationId, repositoryId, catalog.getPartitionMetadataName(), businessTimeStr);
                    indexName = String.format("%s_%d", indexSetName.substring(0, indexSetName.lastIndexOf("_")), 1);

                    ESUtil.createIndexWithoutType(esClient, indexName);

                    Tool.SleepAWhile(1, 0); // sleep 3 second

                    if (!ESUtil.isIndexExists(esClient, indexName)) {
                        log.error(" index " + indexName + " not exists!!! ");
                        continue;
                    }

                    for (int i = 0; i < 3; i++) {
                        ESUtil.addIndexToAlias(esClient, indexSetName, indexName, "add");
                        Tool.SleepAWhile(1, 0); // sleep 3 second
                    }

                    try {
                        if (em.getTransaction().isActive() == false) {
                            em.getTransaction().begin();
                        }

                        // inser new index recordj
                        indexSet = new IndexSet();
                        indexSet.setName(indexSetName);
                        indexSet.setOrganizationId(catalog.getOrganizationId());
                        indexSet.setCatalogId(catalog.getId());
                        indexSet.setPartitionMetadataValue(businessTimeStr);
                        indexSet.setCurrentIndexNumber(1);

                        em.persist(indexSet);
                        em.getTransaction().commit();
                    } catch (Exception e) {
                        if (e.getCause() instanceof ConstraintViolationException) {
                            log.info("already has indexset record, indexName=" + indexSetName);
                            break;
                        }

                        log.error("save indexset failed, retry !!! indexName=" + indexSetName + " e=" + e);

                        if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }

                        Tool.SleepAWhile(1, 0);
                        continue;
                    }

                    return indexName;
                } else {
                    indexSet = indexSetList.iterator().next();
                }

                break;
            }

            return ESUtil.getTargetDocumentIndexName(esClient, indexSet, indexType, documentId);
        } catch (Exception e) {
            log.error("getTargetIndexName() failed! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    public static String getTargetIndexName1(int organizationId, int repositoryId, Client esClient, EntityManager em, int catalogId, Date businessTime, String indexType, String documentId) throws Exception {
        IndexSet indexSet = null;
        String sql = null;
        String indexName;
        String indexSetName;
        String businessTimeStr = null;

        Catalog catalog = em.find(Catalog.class, catalogId);

        if (catalog == null) {
            log.error(" catalog id not exists! ");
        }

        if (businessTime == null) {
            if (catalog.getPartitionMetadataName().equals("data_event_yearmonth")) {
                businessTimeStr = "000000";
            } else if (catalog.getPartitionMetadataName().equals("data_event_yearmonthday")) {
                businessTimeStr = "00000000";
            } else if (catalog.getPartitionMetadataName().equals("data_event_year")) {
                businessTimeStr = "0000";
            } else {
                businessTimeStr = "0000";
            }

            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        } else if (catalog.getPartitionMetadataName().equals("data_event_yearmonth")) {
            businessTimeStr = Tool.getYearMonthStr(businessTime);
            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        } else if (catalog.getPartitionMetadataName().equals("data_event_yearmonthday")) {
            businessTimeStr = Tool.getYearMonthDayStr(businessTime);
            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        } else if (catalog.getPartitionMetadataName().equals("data_event_year")) {
            businessTimeStr = Tool.getYearStr(businessTime);
            sql = String.format("from IndexSet where catalogId=%d and partitionMetadataValue='%s'", catalog.getId(), businessTimeStr);
        }

        try {
            while (true) {
                List<IndexSet> indexSetList = em.createQuery(sql).getResultList();

                if (indexSetList.isEmpty()) {
                    // create new index in search engine
                    indexSetName = Util.getIndexSetName(organizationId, repositoryId, catalog.getPartitionMetadataName(), businessTimeStr);
                    indexName = String.format("%s_%d", indexSetName.substring(0, indexSetName.lastIndexOf("_")), 1);

                    ESUtil.createIndexWithoutType(esClient, indexName);

                    Tool.SleepAWhile(1, 30); // sleep 3 second

                    if (!ESUtil.isIndexExists(esClient, indexName)) {
                        log.error(" index " + indexName + " not exists!!! ");
                        continue;
                    }

                    for (int i = 0; i < 3; i++) {
                        ESUtil.addIndexToAlias(esClient, indexSetName, indexName, "add");
                        Tool.SleepAWhile(1, 0); // sleep 3 second
                    }

                    try {
                        if (em.getTransaction().isActive() == false) {
                            em.getTransaction().begin();
                        }

                        // inser new index recordj
                        indexSet = new IndexSet();
                        indexSet.setName(indexSetName);
                        indexSet.setOrganizationId(catalog.getOrganizationId());
                        indexSet.setCatalogId(catalog.getId());
                        indexSet.setPartitionMetadataValue(businessTimeStr);
                        indexSet.setCurrentIndexNumber(1);

                        em.persist(indexSet);
                        em.getTransaction().commit();
                    } catch (Exception e) {
                        if (e.getCause() instanceof ConstraintViolationException) {
                            log.info("already has indexset record, indexName=" + indexSetName);
                            break;
                        }

                        log.error("save indexset failed, retry !!! indexName=" + indexSetName + " e=" + e);

                        if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }

                        Tool.SleepAWhile(1, 0);
                        continue;
                    }

                    return indexName;
                } else {
                    indexSet = indexSetList.iterator().next();
                }

                break;
            }

            return ESUtil.getTargetDocumentIndexName(esClient, indexSet, indexType, documentId);
        } catch (Exception e) {
            log.error("getTargetIndexName() failed! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    public static long saveDataobjectContentInfo(int organizationId, int repositoryId, Content content, EntityManager platformEm, EntityManager em) throws Exception {
        long ret = 0;

        try {
            Client esClient = ESUtil.getClient(em, platformEm, repositoryId, false);

            String indexName = String.format("datastore_%d_%d_edf", organizationId, repositoryId);
            String indexTypeName = CommonKeys.INDEX_TYPE_FOR_CONTENT_INFO;

            String recordId = content.getId();

            Map<String, Object> jsonMap = new HashMap<>();

            jsonMap.put("content_id", content.getId());
            jsonMap.put("stored_time", new Date());
            jsonMap.put("organization_id", organizationId);
            jsonMap.put("is_content_in_system", content.getIsContentInSystem() == 1);
            jsonMap.put("location", content.getLocation());
            jsonMap.put("encoding", content.getEncoding());
            jsonMap.put("mime_type", content.getMimeType());
            jsonMap.put("object_storage_service_instance_id", content.getObjectStorageServiceInstanceId());
            jsonMap.put("count", 0);

            while (true) {
                try {
                    ret = ESUtil.persistRecord(esClient, indexName, indexTypeName, recordId, jsonMap, 0);
                    break;
                } catch (Exception ee) {
                    if (ee instanceof TypeMissingException) {
                        List<Map<String, String>> definition = prepareContentInfoIndexTypeDefinition();
                        XContentBuilder typeMapping = ESUtil.createTypeMapping(indexTypeName, definition);
                        ESUtil.putMappingToIndex(esClient, indexName, indexTypeName, typeMapping);
                        continue;
                    }

                    throw ee;
                }
            }
        } catch (Exception e) {
            log.error(" saveContent() failed! e=" + e);
            throw e;
        }

        return ret;
    }

    public static String getContentIdByName(DataobjectVO dataobjectVO, String contentName) {
        for (Map<String, Object> content : dataobjectVO.getContents()) {
            String cn = (String) content.get("content_name");

            if (cn.equals(contentName)) {
                return (String) content.get("content_id");
            }
        }

        return "";
    }

    public static Content getDataobjectContentInfo(EntityManager em, EntityManager platformEm, int organizationId, int repositoryId, String contentId) throws Exception {
        Content content = null;

        try {
            Client esClient = ESUtil.getClient(em, platformEm, repositoryId, false);

            String indexName = String.format("datastore_%d_%d_edf", organizationId, repositoryId);
            String indexTypeName = CommonKeys.INDEX_TYPE_FOR_CONTENT_INFO;

            JSONObject jsonObject = ESUtil.findRecord(esClient, indexName, indexTypeName, contentId);

            if (jsonObject == null) {
                return null;
            }

            content = new Content();
            content.setId(contentId);
            content.setOrganizationId(organizationId);
            content.setIsContentInSystem(jsonObject.optBoolean("is_content_in_system") ? (short) 1 : 0);
            content.setEncoding(jsonObject.optString("encoding"));
            content.setMimeType(jsonObject.optString("mime_type"));
            content.setLocation(jsonObject.optString("location"));
            content.setCount(jsonObject.optInt("count"));

            String time = (String) jsonObject.opt("stored_time");
            content.setStoredTime(Tool.convertESDateStringToDate(time));

            content.setObjectStorageServiceInstanceId(jsonObject.optInt("object_storage_service_instance_id"));
        } catch (Exception e) {
            log.error(" saveContent() failed! e=" + e + " stacktrace=" + e.getStackTrace());
            throw e;
        }

        return content;
    }

    private static List<Map<String, String>> prepareContentInfoIndexTypeDefinition() {
        Map<String, String> map;
        List<Map<String, String>> fieldList = new ArrayList<>();

        map = new HashMap<>();
        map.put("name", "organization_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "content_id");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "stored_time");
        map.put("dataType", MetadataDataType.TIMESTAMP.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "is_content_in_system");
        map.put("dataType", MetadataDataType.BOOLEAN.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "location");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "encoding");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "mime_type");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "object_storage_service_instance_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "count");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        return fieldList;
    }

    /*
                
    private static List<Map<String,String>> prepareMetricsTagDataIndexTypeDefinition()
    {
        Map<String,String> map;
        List<Map<String,String>> fieldList = new ArrayList<>();
        
        map = new HashMap<>();
        map.put("name", "organization_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);
        
        map = new HashMap<>();
        map.put("name", "entity_type");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);
        
        map = new HashMap<>();
        map.put("name", "entity_id");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);
        
        map = new HashMap<>();
        map.put("name", "business_time_string");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);       
        
        map = new HashMap<>();
        map.put("name", "metrics_or_tag_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);           
        
        map = new HashMap<>();
        map.put("name", "metrics_or_tag_short_name");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);            
        
        map = new HashMap<>();
        map.put("name", "value");
        map.put("dataType", MetadataDataType.DOUBLE.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);            
        
        map = new HashMap<>();
        map.put("name", "is_tag_value");
        map.put("dataType", MetadataDataType.BOOLEAN.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);                    
        
        map = new HashMap<>();
        map.put("name", "last_computing_time");
        map.put("dataType", MetadataDataType.BOOLEAN.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);                    
                
        return fieldList;
    }
     */
    public static long getEntityCount(int organizationId, String entityName, EntityManager em) {
        String sql;
        long count;

        sql = String.format("select count(*) from %s t where t.organizationId=%d", entityName, organizationId);
        count = (Long) em.createQuery(sql).getSingleResult();
        return count;
    }

    public static String removeUnExpectedChar(String line, String seperatorStr, String replaceSeperatorStr) {
        char seperator = seperatorStr.charAt(0);
        char replaceSeperator = replaceSeperatorStr.charAt(0);

        StringBuilder builder = new StringBuilder();
        boolean inDoubleQuotate = false;

        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '"') {
                inDoubleQuotate = !inDoubleQuotate;
            }

            if (inDoubleQuotate) {
                if (line.charAt(i) == seperator) {
                    builder.append(replaceSeperator);
                } else {
                    builder.append(line.charAt(i));
                }
            } else {
                builder.append(line.charAt(i));
            }
        }

        return builder.toString();
    }

    public static String getAggregationTime(String datetimeValue, int aggregationType) {
        String showFormat = TimeAggregationType.findByValue(aggregationType).getShowFormat();
        return String.format(showFormat, datetimeValue);
    }

    public static ProgramMetricsCalculationBase getProgramMetricsCalculationClass(String className, EntityMetricsCalculator emCaculator) throws Exception {
        Object[] params;
        Class[] paramDef;
        Constructor constructor;

        Class processorClass = Class.forName(className);

        if (emCaculator != null) {
            paramDef = new Class[]{EntityMetricsCalculator.class};
            constructor = processorClass.getConstructor(paramDef);
            params = new Object[]{emCaculator};
        } else {
            paramDef = new Class[]{};
            constructor = processorClass.getConstructor(paramDef);
            params = new Object[]{};
        }

        ProgramMetricsCalculationBase programMetricsCalculator = (ProgramMetricsCalculationBase) constructor.newInstance(params);
        return programMetricsCalculator;
    }

    public static DataProcessingExtensionBase getDataProcessingExtensionClass(String dataProcessingExtensionIdClassName, DataService.Client dataService, Datasource datasource, Job job) {
        DataProcessingExtensionBase dataProcessingExtensionInstance = null;

        try {
            if (dataProcessingExtensionIdClassName == null || dataProcessingExtensionIdClassName.trim().isEmpty()) {
                return null;
            }

            Class processorClass = Class.forName(CommonKeys.DATA_PROCESSING_EXTENSION_PACKAGE_NAME + "." + dataProcessingExtensionIdClassName);

            Class[] paramDef = new Class[]{DataService.Client.class, Datasource.class, Job.class};
            Constructor constructor = processorClass.getConstructor(paramDef);
            Object[] params = new Object[]{dataService, datasource, job};

            dataProcessingExtensionInstance = (DataProcessingExtensionBase) constructor.newInstance(params);
        } catch (Exception e) {
            log.error(" getDataProcessingExtensionClass() failed! e=" + e + " dataProcessingExtensionIdClassName=" + dataProcessingExtensionIdClassName);
            return null;
        }

        return dataProcessingExtensionInstance;
    }

    public static Date getQueryTime(EntityManager em, EntityManager platformEm, String type, DataService.Client dataService, AnalyticDataview dataview, int organizationId, int repositoryId, int catalogId, String indexType) throws Exception {
        SearchRequest request;
        SearchResponse response;

        request = new SearchRequest(organizationId, repositoryId, catalogId, SearchType.SEARCH_GET_ONE_PAGE.getValue());

        if (type.equals("startTime")) {
            request.setSortColumns(String.format("%s,ASCEND", dataview.getTimeAggregationColumn().trim()));
        } else {
            request.setSortColumns(String.format("%s,DESCEND", dataview.getTimeAggregationColumn().trim()));
        }

        request.setTargetIndexKeys(null);
        request.setTargetIndexTypes(Arrays.asList(indexType));
        request.setFilterStr(dataview.getFilterString());
        request.setSearchFrom(0);
        request.setMaxExpectedHits(1);    // only get first one
        request.setMaxExecutionTime(CommonKeys.MAX_EXPECTED_EXCUTION_TIME);
        request.setColumns(dataview.getTimeAggregationColumn().trim());

        if (dataService != null) {
            response = dataService.getDataobjects(request);
        } else {
            response = SearchUtil.getDataobjects(em, platformEm, request);
        }

        List<Map<String, String>> results = response.getSearchResults();

        if (results == null || results.isEmpty()) {
            return null;
        }

        Map<String, String> result = results.get(0);
        JSONObject resultFields = new JSONObject(result.get("fields"));
        String dateStr = resultFields.optString(dataview.getTimeAggregationColumn().trim());

        return Tool.convertESDateStringToDate(dateStr);
    }

    public static void addTimeRangeForDatasetAggregation(List<Map<String, String>> searchResults, Date startTime, String columnName) {
        for (Map<String, String> searchResult : searchResults) {
            try {
                searchResult.put(columnName, Tool.convertDateToTimestampStringWithMilliSecond(startTime));
                //searchResult.put("end_time", Tool.convertDateToTimestampStringWithMilliSecond(endTime));
            } catch (Exception e) {
                log.error("addTimeRangeForDatasetAggregation() failed!  id = " + e);
                throw e;
            }
        }
    }

    public static List<Map<String, Object>> getTimePeriod(AnalyticDataview dataview, Date startTime, Date endTime) {
        Date newStartTime;
        Date newEndTime;
        Map<String, Object> map;
        Date sDate;
        Date eDate;
        int diff;
        int i;

        List<Map<String, Object>> periodList = new ArrayList<>();

        startTime = Tool.startOfSecond(startTime);  // remove mili second

        DateTime dt1 = new DateTime(startTime);
        DateTime dt2 = new DateTime(endTime);

        TimeAggregationType type = TimeAggregationType.findByValue(dataview.getTimeAggregationType());

        if (type == TimeAggregationType.SECOND) {
            diff = Seconds.secondsBetween(dt1, dt2).getSeconds() + 1;
            i = 0;

            while (i * dataview.getTimeAggregationLength() < diff) {
                newStartTime = Tool.dateAddDiff(startTime, i * dataview.getTimeAggregationLength(), Calendar.SECOND);
                newEndTime = Tool.dateAddDiff(startTime, (i + 1) * dataview.getTimeAggregationLength() - 1, Calendar.SECOND);

                sDate = Tool.startOfSecond(newStartTime);
                eDate = Tool.endOfSecond(newEndTime);

                map = generatePeriod(dataview, sDate, eDate);
                periodList.add(map);

                i++;
            }
        } else if (type == TimeAggregationType.MINUTE) {
            diff = Minutes.minutesBetween(dt1, dt2).getMinutes() + 1;
            i = 0;

            while (i * dataview.getTimeAggregationLength() < diff) {
                newStartTime = Tool.dateAddDiff(startTime, i * dataview.getTimeAggregationLength(), Calendar.MINUTE);
                newEndTime = Tool.dateAddDiff(startTime, (i + 1) * dataview.getTimeAggregationLength() - 1, Calendar.MINUTE);

                sDate = Tool.startOfMinute(newStartTime);
                eDate = Tool.endOfMinute(newEndTime);

                map = generatePeriod(dataview, sDate, eDate);
                periodList.add(map);

                i++;
            }
        } else if (type == TimeAggregationType.HOUR) {
            diff = Hours.hoursBetween(dt1, dt2).getHours() + 1;
            i = 0;

            while (i * dataview.getTimeAggregationLength() < diff) {
                newStartTime = Tool.dateAddDiff(startTime, i * dataview.getTimeAggregationLength(), Calendar.HOUR);
                newEndTime = Tool.dateAddDiff(startTime, (i + 1) * dataview.getTimeAggregationLength() - 1, Calendar.HOUR);

                sDate = Tool.startOfHour(newStartTime);
                eDate = Tool.endOfHour(newEndTime);

                map = generatePeriod(dataview, sDate, eDate);
                periodList.add(map);

                i++;
            }
        } else if (type == TimeAggregationType.DAY) {
            diff = Days.daysBetween(dt1, dt2).getDays() + 1;
            i = 0;

            while (i * dataview.getTimeAggregationLength() < diff) {
                newStartTime = Tool.dateAddDiff(startTime, i * dataview.getTimeAggregationLength(), Calendar.DATE);
                newEndTime = Tool.dateAddDiff(startTime, (i + 1) * dataview.getTimeAggregationLength() - 1, Calendar.DATE);

                sDate = Tool.startOfDay(newStartTime);
                eDate = Tool.endOfDay(newEndTime);

                map = generatePeriod(dataview, sDate, eDate);
                periodList.add(map);

                i++;
            }
        } else if (type == TimeAggregationType.MONTH) {
            diff = Months.monthsBetween(dt1, dt2).getMonths() + 1;
            i = 0;

            while (i * dataview.getTimeAggregationLength() < diff) {
                newStartTime = Tool.dateAddDiff(startTime, i * dataview.getTimeAggregationLength(), Calendar.MONTH);
                newEndTime = Tool.dateAddDiff(startTime, (i + 1) * dataview.getTimeAggregationLength() - 1, Calendar.MONTH);

                sDate = Tool.startOfMonth(newStartTime);
                eDate = Tool.endOfMonth(newEndTime);

                map = generatePeriod(dataview, sDate, eDate);
                periodList.add(map);

                i++;
            }
        } else if (type == TimeAggregationType.YEAR) {
            diff = Years.yearsBetween(dt1, dt2).getYears() + 1;
            i = 0;

            while (i * dataview.getTimeAggregationLength() < diff) {
                newStartTime = Tool.dateAddDiff(startTime, i * dataview.getTimeAggregationLength(), Calendar.YEAR);
                newEndTime = Tool.dateAddDiff(startTime, (i + 1) * dataview.getTimeAggregationLength() - 1, Calendar.YEAR);

                sDate = Tool.startOfYear(newStartTime);
                eDate = Tool.endOfYear(newEndTime);

                map = generatePeriod(dataview, sDate, eDate);
                periodList.add(map);

                i++;
            }
        }

        return periodList;
    }

    private static Map<String, Object> generatePeriod(AnalyticDataview dataview, Date sDate, Date eDate) {
        Map<String, Object> map;
        map = new HashMap<>();
        map.put(("startTime"), sDate);
        String timeRangeStr = String.format("%s:[%s TO %s]", dataview.getTimeAggregationColumn(), Tool.convertDateToTimestampStringForES(sDate), Tool.convertDateToTimestampStringForES(eDate));
        map.put("timeRangeStr", timeRangeStr);
        return map;
    }

    public static String getSlaveFilterStr(DataobjectVO dataobjectVO, DataobjectTypeAssociation dta) {
        StringBuilder builder = new StringBuilder();

        if (dta.getKeyMapping() == null || dta.getKeyMapping().trim().isEmpty()) {
            return "";
        }

        String[] keyMappings = dta.getKeyMapping().split("\\;");  // master_columns1,slave_columns1; master_column2, slave_column2;...

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");

            String masterValue = dataobjectVO.getMetadatas().get(map[0]);

            if (masterValue == null || masterValue.trim().isEmpty()) {
                masterValue = dataobjectVO.getThisDataobjectTypeMetadatas().get(map[0]);
            }

            if (masterValue == null || masterValue.trim().isEmpty()) {
                return "";
            }

            builder.append(map[1]).append(":").append("\"").append(masterValue).append("\"");

            if (i + 1 < keyMappings.length) {
                builder.append(" AND ");
            }
        }

        return builder.toString();
    }

    public static Map<String, String> getMutipleSlaveFilterStr(List<DataobjectVO> dataobjectVOs, DataobjectTypeAssociation dta) {
        Map<String, String> ret = new HashMap<>();
        StringBuilder builder = new StringBuilder();

        if (dta.getKeyMapping() == null || dta.getKeyMapping().trim().isEmpty()) {
            return null;
        }

        String[] keyMappings = dta.getKeyMapping().split("\\;");  // master_columns1,slave_columns1; master_column2, slave_column2;...

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");

            builder.append(map[1]).append(":( ");

            ret.put("masterColumnName", map[0]);
            ret.put("slaveColumnName", map[1]);

            for (int j = 0; j < dataobjectVOs.size(); j++) {
                String masterValue = dataobjectVOs.get(j).getMetadatas().get(map[0]);

                if (masterValue == null || masterValue.trim().isEmpty()) {
                    masterValue = dataobjectVOs.get(j).getThisDataobjectTypeMetadatas().get(map[0]);
                }

                if (masterValue == null || masterValue.trim().isEmpty()) {
                    continue;
                }

                builder.append("\"").append(masterValue).append("\"").append(" ");

                //if ( j+1 < dataobjectVOs.size())
                //    builder.append(", ");
            }

            builder.append(" ) ");

            if (i + 1 < keyMappings.length) {
                builder.append(" AND ");
            }
        }

        ret.put("filterStr", builder.toString());

        return ret;
    }

    public static Map<String, String> getMutipleSlaveFilterStrFromRow(List<Map<String, String>> rows, DataobjectTypeAssociation dta) {
        Map<String, String> ret = new HashMap<>();
        StringBuilder builder = new StringBuilder();

        if (dta.getKeyMapping() == null || dta.getKeyMapping().trim().isEmpty()) {
            return null;
        }

        String[] keyMappings = dta.getKeyMapping().split("\\;");  // master_columns1,slave_columns1; master_column2, slave_column2;...

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");

            builder.append(map[1]).append(":( ");

            ret.put("masterColumnName", map[0]);
            ret.put("slaveColumnName", map[1]);

            for (int j = 0; j < rows.size(); j++) {
                String masterValue = rows.get(j).get(map[0]);

                if (masterValue == null || masterValue.trim().isEmpty()) {
                    continue;
                }

                builder.append("\"").append(masterValue).append("\"").append(" ");

                //if ( j+1 < dataobjectVOs.size())
                //    builder.append(", ");
            }

            builder.append(" ) ");

            if (i + 1 < keyMappings.length) {
                builder.append(" AND ");
            }
        }

        ret.put("filterStr", builder.toString());

        return ret;
    }

    public static Map<String, Object> getMutipleSlaveFilterStrFromRowNew(List<Map<String, String>> rows, DataobjectTypeAssociation dta) {
        Map<String, Object> ret = new HashMap<>();
        StringBuilder builder = new StringBuilder();
        List<String> masterColumnNames = new ArrayList<>();
        List<String> slaveColumnNames = new ArrayList<>();
        Set masterColumnNamesSet = new HashSet<>();
        String flag;

        if (dta.getKeyMapping() == null || dta.getKeyMapping().trim().isEmpty()) {
            return null;
        }

        String[] keyMappings = dta.getKeyMapping().split("\\;");  // master_columns1,slave_columns1; master_column2, slave_column2;...

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");
            masterColumnNamesSet.add(map[0]);
        }

        if (masterColumnNamesSet.size() < keyMappings.length) // if it's one to more, condition should be OR
        {
            flag = " OR ";
        } else {
            flag = " AND ";
        }

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");

            builder.append(map[1]).append(":( ");

            masterColumnNames.add(map[0]);
            slaveColumnNames.add(map[1]);

            for (int j = 0; j < rows.size(); j++) {
                String masterValue = rows.get(j).get(map[0]);

                if (masterValue == null || masterValue.trim().isEmpty()) {
                    continue;
                }

                builder.append("\"").append(masterValue).append("\"").append(" ");
            }

            builder.append(" ) ");

            if (i + 1 < keyMappings.length) {
                builder.append(flag);
            }
        }

        ret.put("filterStr", builder.toString());
        ret.put("masterColumnNames", masterColumnNames);
        ret.put("slaveColumnNames", slaveColumnNames);
        ret.put("flag", flag);

        return ret;
    }

    public static Map<String, Object> getMutipleMasterFilterStrFromRowNew(List<Map<String, String>> rows, DataobjectTypeAssociation dta) {
        Map<String, Object> ret = new HashMap<>();
        StringBuilder builder = new StringBuilder();
        List<String> masterColumnNames = new ArrayList<>();
        List<String> slaveColumnNames = new ArrayList<>();
        Set masterColumnNamesSet = new HashSet<>();
        String flag;

        if (dta.getKeyMapping() == null || dta.getKeyMapping().trim().isEmpty()) {
            return null;
        }

        String[] keyMappings = dta.getKeyMapping().split("\\;");  // master_columns1,slave_columns1; master_column2, slave_column2;...

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");
            masterColumnNamesSet.add(map[1]);
        }

        if (masterColumnNamesSet.size() < keyMappings.length) // if it's one to more, condition should be OR
        {
            flag = " OR ";
        } else {
            flag = " AND ";
        }

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");

            builder.append(map[0]).append(":( ");

            masterColumnNames.add(map[1]);
            slaveColumnNames.add(map[0]);

            for (int j = 0; j < rows.size(); j++) {
                String masterValue = rows.get(j).get(map[1]);

                if (masterValue == null || masterValue.trim().isEmpty()) {
                    continue;
                }

                builder.append("\"").append(masterValue).append("\"").append(" ");
            }

            builder.append(" ) ");

            if (i + 1 < keyMappings.length) {
                builder.append(flag);
            }
        }

        ret.put("filterStr", builder.toString());
        ret.put("masterColumnNames", masterColumnNames);
        ret.put("slaveColumnNames", slaveColumnNames);
        ret.put("flag", flag);

        return ret;
    }

    public static String getSlaveFilterStrFromDatasetRow(Map<String, String> datasetRow, DataobjectTypeAssociation dta) {
        StringBuilder builder = new StringBuilder();

        if (dta.getKeyMapping() == null || dta.getKeyMapping().trim().isEmpty()) {
            return "";
        }

        String[] keyMappings = dta.getKeyMapping().split("\\;");  // master_columns1,slave_columns1; master_column2, slave_column2;...

        for (int i = 0; i < keyMappings.length; i++) {
            String[] map = keyMappings[i].split("\\,");
            String masterValue = datasetRow.get(map[0]);

            if (masterValue == null || masterValue.trim().isEmpty()) {
                return "";
            }

            builder.append(map[1]).append(":").append("\"").append(masterValue).append("\"");

            if (i + 1 < keyMappings.length) {
                builder.append(" AND ");
            }
        }

        return builder.toString();
    }

    /*public static String getSameEventFilterStr(DataobjectVO dataobjectVO,DataobjectTypeAssociation dta)
    {
        StringBuilder builder = new StringBuilder();
        
        if ( dta.getSameEventColumns()== null || dta.getSameEventColumns().trim().isEmpty() )
            return "";
        
        String[] relationshipColumns = dta.getSameEventColumns().split("\\,");  // master_columns1,slave_columns1; master_column2, slave_column2;...
        
        for(int i=0;i<relationshipColumns.length;i++)
        {
            String value = dataobjectVO.getMetadatas().get(relationshipColumns[i]);
            
            if ( value == null || value.trim().isEmpty() )
                value = dataobjectVO.getThisDataobjectTypeMetadatas().get(relationshipColumns[i]);

            if ( value == null || value.trim().isEmpty() )
                return "";
                    
            builder.append(relationshipColumns[i]).append(":").append("\"").append(value).append("\"");
            
            if ( i+1 < relationshipColumns.length )
                builder.append(" AND ");
        }
        
        return builder.toString();
    }*/
    public static int getMetadataDataTypeString(String metadataName, List<Map<String, Object>> dataobjectTypeMetadataInfoList) {
        for (Map<String, Object> definition : dataobjectTypeMetadataInfoList) {
            String name = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

            if (!name.toLowerCase().equals(metadataName.toLowerCase())) {
                continue;
            }

            return Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
        }

        return 0;
    }

    public static int getMetadataDataTypeString1(String metadataName, List<Map<String, Object>> dataobjectTypeMetadataInfoList, String dataobjectTypeName) {
        for (Map<String, Object> definition : dataobjectTypeMetadataInfoList) {
            String name = (String) definition.get(CommonKeys.METADATA_NODE_NAME);

            name = Tool.changeTableColumnName(dataobjectTypeName.trim(), name, true);

            if (!name.toLowerCase().equals(metadataName.toLowerCase())) {
                continue;
            }

            return Integer.parseInt((String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE));
        }

        return 0;
    }

    public static String getDataServiceIPs(EntityManager platformEm, int organizationId) throws Exception {
        List<ServiceInstance> siList;

        try {
            String sql = String.format("from ServiceInstance si where si.id in (select osi.serviceInstanceId from OrganizationServiceInstance osi where osi.organizationServiceInstancePK.organizationId = %d)", organizationId);
            siList = platformEm.createQuery(sql).getResultList();

            for (ServiceInstance instance : siList) {
                if (instance.getType() == PlatformServiceType.EDF_DATA_SERVICE.getValue()) {
                    return instance.getLocation();
                }
            }

            log.error("EDF_DATA_SERVICE Instance not found!");
            throw new Exception("EDF_DATA_SERVICE Instance not found");
        } catch (Exception e) {
            log.error("Failed to getDataServiceIPs()!  e=" + e.getMessage());
            throw e;
        }
    }

    public static String getMQServiceIPs(EntityManager platformEm, int organizationId) throws Exception {
        List<ServiceInstance> siList;

        try {
            String sql = String.format("from ServiceInstance si where si.id in (select osi.serviceInstanceId from OrganizationServiceInstance osi where osi.organizationServiceInstancePK.organizationId = %d)", organizationId);
            siList = platformEm.createQuery(sql).getResultList();

            for (ServiceInstance instance : siList) {
                if (instance.getType() == PlatformServiceType.MQ_SERVICE.getValue()) {
                    return instance.getLocation();
                }
            }

            log.error("MQ_SERVICE Instance not found!");
            throw new Exception("MQ_SERVICE Instance not found");
        } catch (Exception e) {
            log.error("Failed to getMQServiceIPs()!  e=" + e.getMessage());
            throw e;
        }
    }

    public static String getCommonServiceIPs(EntityManager platformEm, int organizationId) throws Exception {
        List<ServiceInstance> siList;

        try {
            String sql = String.format("from ServiceInstance si where si.id in (select osi.serviceInstanceId from OrganizationServiceInstance osi where osi.organizationServiceInstancePK.organizationId = %d)", organizationId);
            siList = platformEm.createQuery(sql).getResultList();

            for (ServiceInstance instance : siList) {
                if (instance.getType() == PlatformServiceType.EDF_COMMON_SERVICE.getValue()) {
                    return instance.getLocation();
                }
            }

            log.error("COMMON_SERVICE Instance not found!");
            throw new Exception("COMMON_SERVICE Instance not found");
        } catch (Exception e) {
            log.error("Failed to getCommonServiceIPs()!  e=" + e.getMessage());
            throw e;
        }
    }

    public static String getImageServiceIPs(EntityManager platformEm, int organizationId) throws Exception {
        List<ServiceInstance> siList;

        try {
            String sql = String.format("from ServiceInstance si where si.id in (select osi.serviceInstanceId from OrganizationServiceInstance osi where osi.organizationServiceInstancePK.organizationId = %d)", organizationId);
            siList = platformEm.createQuery(sql).getResultList();

            for (ServiceInstance instance : siList) {
                if (instance.getType() == PlatformServiceType.EDF_COMMON_SERVICE.getValue()) {
                    return instance.getLocation();
                }
            }

            log.error("COMMON_SERVICE Instance not found!");
            throw new Exception("COMMON_SERVICE Instance not found");
        } catch (Exception e) {
            log.error("Failed to getCommonServiceIPs()!  e=" + e.getMessage());
            throw e;
        }
    }

    public static String getDataServiceIP(EntityManager platformEm, int organizationId) throws Exception {
        String dataServiceIP = null;

        try {
            String dataServiceIPs = getDataServiceIPs(platformEm, organizationId);
            dataServiceIP = pickOneServiceIP(dataServiceIPs);
        } catch (Exception e) {
            log.error("Failed to getDataServiceIP()!  e=" + e.getMessage());
            throw e;
        }

        return dataServiceIP;
    }

    public static String pickOneServiceIP(String serviceIPs) {
        String serviceIP = serviceIPs;

        try {
            String[] serviceIPArrays = serviceIPs.split("\\,");
            int len = serviceIPArrays.length;

            Random random = new Random();
            int index = random.nextInt(len);

            serviceIP = serviceIPArrays[index];
        } catch (Exception e) {
            log.error("Util.pickOneServiceIP() failed! e=" + e + " serviceIPs=" + serviceIPs);
        }

        return serviceIP;
    }

    public static Map<String, String> getConfigPropertyMap(String config) {
        Map<String, String> properties = new HashMap<>();

        if (config == null || config.trim().isEmpty()) {
            return properties;
        }

        Document metadataXmlDoc = Tool.getXmlDocument(config);
        List<Element> nodes = metadataXmlDoc.selectNodes("//config/property");

        for (Element node : nodes) {
            properties.put(node.element("name").getTextTrim(), node.element("value").getTextTrim());
        }

        return properties;
    }

    public static List<Map<String, String>> getConfigPropertyList(String config) {
        List<Map<String, String>> properties = new ArrayList<>();

        if (config == null || config.trim().isEmpty()) {
            return properties;
        }

        Document metadataXmlDoc = Tool.getXmlDocument(config);
        List<Element> nodes = metadataXmlDoc.selectNodes("//config/property");

        for (Element node : nodes) {
            Map<String, String> property = new HashMap<>();

            property.put("name", node.element("name").getTextTrim());
            property.put("value", node.element("value").getTextTrim());

            properties.add(property);
            property.put("id", String.valueOf(properties.indexOf(property)));
        }

        return properties;
    }

    public static Map<String, Object> getGroupRoleUserForASingleUser(EntityManager em, UserType userType, int userId) {
        String sql;
        Map<String, Object> map = new HashMap<>();

        try {
            map.put("userType", userType.getValue());

            if (userType == UserType.USER_GROUP) {
                sql = String.format("select ug.userGroupPK.groupId from UserGroup ug where ug.userGroupPK.userId=%d", userId);
                List<Integer> groupIdList = em.createQuery(sql).getResultList();

                map.put("userIds", groupIdList.toArray(new Integer[0]));
            } else {
                sql = String.format("select ur.userRolePK.roleId from UserRole ur where ur.userRolePK.userId=%d", userId);
                List<Integer> roleIdList = em.createQuery(sql).getResultList();
                map.put("userIds", roleIdList.toArray(new Integer[0]));
            }

            return map;
        } catch (Exception e) {
            log.error(" getGroupRoleUserForASingleUser failed! e=" + e);
            throw e;
        }
    }

    public static String getTargetDataobjectMetadataName(StructuredFileDefinition fileDefinition, List<Map<String, Object>> columnDefinitions, int fieldNo) {
        String metadataName = null;

        for (Map<String, Object> definition : columnDefinitions) {
            if (fieldNo == Integer.parseInt((String) definition.get("id"))) {
                if (fileDefinition.getCreateDataobjectType() == 1) {
                    metadataName = (String) definition.get("name");
                } else {
                    metadataName = (String) definition.get("dataobjectTypeFieldName");
                }
                break;
            }
        }

        return metadataName;

    }

    public static boolean getColumnNeedToProcessFlag(List<Map<String, Object>> columnDefinitions, int fieldNo) {
        boolean needToProcess = false;

        for (Map<String, Object> definition : columnDefinitions) {
            if (fieldNo == Integer.parseInt((String) definition.get("id"))) {
                needToProcess = (Boolean) definition.get("needToProcess");
            }
        }

        return needToProcess;
    }

    public static String getPrimaryKeyValue(List<Integer> primaryKeyFieldIds, StructuredFileDefinition fileDefinition, String[] fields, String lineHash) {
        StringBuilder value = new StringBuilder();

        if (primaryKeyFieldIds == null || primaryKeyFieldIds.isEmpty()) {
            for (String fieldStr : fields) {
                value.append(fieldStr.trim()).append(".");
            }
        } else {
            for (Integer fieldId : primaryKeyFieldIds) {
                String fieldStr = fields[fieldId - 1];

                if (Tool.onlyContainsBlank(fieldStr)) {
                    value.append(fieldStr).append(".");
                } else if (fieldStr == null || fieldStr.trim().isEmpty()) {
                    String key = String.format("%s", lineHash);
                    value.append(key).append(".");
                } else {
                    value.append(fieldStr.trim()).append(".");
                }
            }
        }

        String str = String.valueOf(fileDefinition.getId()) + "." + value.toString();
        return str.substring(0, str.length() - 1);
    }

    public static boolean hasEmptyPrimaryKeyValue(List<Integer> primaryKeyFieldIds, String[] fields) {
        for (Integer fieldId : primaryKeyFieldIds) {
            String fieldStr = fields[fieldId - 1];

            if (fieldStr == null || fieldStr.trim().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    public static List<Integer> getPrimaryKeyFieldIds(String fields) {
        String FIELD_NODE = "//structureDataFileDefinition/column";

        List<Integer> primaryKeyFieldIds = new ArrayList<>();

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for (Element node : nodes) {
                String primaryKeyStr = node.element("isPrimaryKey").getTextTrim();

                if (primaryKeyStr == null || primaryKeyStr.isEmpty()) {
                    continue;
                }

                if (Integer.parseInt(primaryKeyStr) == 1) {
                    primaryKeyFieldIds.add(Integer.parseInt(node.element("id").getTextTrim()));
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return primaryKeyFieldIds;
    }

    public static Map<String, String> getFieldDatatimeformat(String fields) {
        String FIELD_NODE = "//structureDataFileDefinition/column";

        Map<String, String> map = new HashMap<>();

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for (Element node : nodes) {
                String datetimeFormat = node.element("datetimeFormat").getTextTrim();

                if (datetimeFormat == null || datetimeFormat.isEmpty()) {
                    continue;
                }

                String name = node.element("name").getTextTrim();
                map.put(name, datetimeFormat);
            }
        } catch (Exception e) {
            log.error("getPrimaryKeyFieldNames() failed!  e=" + e + " fields=" + fields);
            throw e;
        }

        return map;
    }

    public static void getDataobjectTypePrimaryKeyFieldNames(EntityManager platformEm, int dataobjectTypeId, List<String> primaryKeyFieldNames) {
        DataobjectType dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);

        if (dataobjectType.getParentType() > 1) // exclude dataojbect
        {
            getDataobjectTypePrimaryKeyFieldNames(platformEm, dataobjectType.getParentType(), primaryKeyFieldNames);
        }

        List<String> list = getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());

        primaryKeyFieldNames.addAll(list);
    }

    public static String getDataobjectTypePrimaryKeyValue(List<String> primaryKeyFieldNames, JSONObject dataobjectJson, DataobjectVO dataobjectVO) {
        String val = "";

        for (String primaryKeysFieldName : primaryKeyFieldNames) {
            if (dataobjectJson != null) {
                val += dataobjectJson.optString(primaryKeysFieldName) + ".";
            } else {
                val += dataobjectVO.getThisDataobjectTypeMetadatas().get(primaryKeysFieldName) + ".";
            }
        }
        if (!val.isEmpty()) {
            val = val.substring(0, val.length() - 1);
        }

        return val;
    }

    public static String getDataobjectTypeFieldValue(String FieldName, JSONObject dataobjectJson, DataobjectVO dataobjectVO) {
        String val = "";

        if (dataobjectJson != null) {
            val = dataobjectJson.optString(FieldName);
        } else {
            val = dataobjectVO.getThisDataobjectTypeMetadatas().get(FieldName);
        }

        return val;
    }

    public static String getNextItemCondition(String lastProcessedItem, int comparingFieldType, String comparingFieldName, DatabaseType databaseType) {
        String condition = "";

        if (lastProcessedItem == null || lastProcessedItem.trim().isEmpty() || lastProcessedItem.equals("null")) {
            if (comparingFieldType == MetadataDataType.DATE.getValue() || comparingFieldType == MetadataDataType.TIMESTAMP.getValue()) {
                lastProcessedItem = "1970-01-01 00:00:00";
            } else {
                lastProcessedItem = "0";
            }
        }

        if (comparingFieldType == MetadataDataType.STRING.getValue()) {
            condition = String.format("%s > '%s'", comparingFieldName, lastProcessedItem);
        } else if (comparingFieldType == MetadataDataType.DATE.getValue()) {
            if ((databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE)) {
                condition = String.format("%s > %s", comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItem, "yyyy-MM-dd"));
            } else {
                condition = String.format("%s > '%s'", comparingFieldName, lastProcessedItem);
            }
        } else if (comparingFieldType == MetadataDataType.TIMESTAMP.getValue()) {
            if ((databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE)) {
                condition = String.format("%s > %s", comparingFieldName, Tool.getOralceDatetimeFormat(lastProcessedItem, "yyyy-MM-dd hh24:mi:ss"));
            } else {
                condition = String.format("%s > '%s'", comparingFieldName, lastProcessedItem);
            }
        } else {
            condition = String.format("%s > %s", comparingFieldName, lastProcessedItem);
        }

        return condition;
    }

    public static List<String> getDataobjectTypePrimaryKeyFieldNames(String fields) {
        String FIELD_NODE = "//dataobjectMetadatas/metadata";

        List<String> primaryKeyFieldNames = new ArrayList<>();

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for (Element node : nodes) {
                if (node.element("isPrimaryKey") == null) {
                    continue;
                }

                String primaryKeyStr = node.element("isPrimaryKey").getTextTrim();

                if (primaryKeyStr == null || primaryKeyStr.isEmpty()) {
                    continue;
                }

                if (Integer.parseInt(primaryKeyStr) == 1) {
                    String name = node.element("name").getTextTrim();
                    primaryKeyFieldNames.add(name);
                }
            }
        } catch (Exception e) {
            log.error("getDataobjectTypePrimaryKeyFieldNames() failed!  e=" + e);
            throw e;
        }

        return primaryKeyFieldNames;
    }

    /*  public static List<String> getPrimaryKeyFieldNames(String fields) 
    {
        String FIELD_NODE = "//structureDataFileDefinition/column";

        List<String> primaryKeyFieldNames = new ArrayList<>();
        
        try
        {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for( Element node: nodes)
            {
                String primaryKeyStr = node.element("isPrimaryKey").getTextTrim();
 
                if ( primaryKeyStr == null || primaryKeyStr.isEmpty())
                    continue;
                
                if ( Integer.parseInt(primaryKeyStr) == 1 )
                {
                    String name = node.element("name").getTextTrim();
                    primaryKeyFieldNames.add(name);
                }
            }
        }
        catch(Exception e)
        {
            log.error("getPrimaryKeyFieldNames() failed!  e="+e+" fields="+fields);
            throw e;
        }
        
        return primaryKeyFieldNames;
    } */
    public static String getPrimaryKeyFieldNameStr(String fields) {
        String FIELD_NODE = "//structureDataFileDefinition/column";

        String primaryKeyFieldNames = "";

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for (Element node : nodes) {
                String primaryKeyStr = node.element("isPrimaryKey").getTextTrim();

                if (primaryKeyStr == null || primaryKeyStr.isEmpty()) {
                    continue;
                }

                if (Integer.parseInt(primaryKeyStr) == 1) {
                    String name = node.element("name").getTextTrim();
                    primaryKeyFieldNames += name + ",";
                }
            }

            if (!primaryKeyFieldNames.isEmpty()) {
                primaryKeyFieldNames = primaryKeyFieldNames.substring(0, primaryKeyFieldNames.length() - 1);
            }
        } catch (Exception e) {
            log.error("getPrimaryKeyFieldNames() failed!  e=" + e + " fields=" + fields);
            throw e;
        }

        return primaryKeyFieldNames;
    }

    public static Map<String, String> getDataobjectTypeColumnLenPrecision(String fields) {
        String FIELD_NODE = "//dataobjectMetadatas/metadata";

        Map<String, String> map = new HashMap<>();

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for (Element node : nodes) {
                String name = node.element("name").getTextTrim();

                if (node.element("length") == null) {
                    continue;
                }

                String sizeStr = node.element("length").getTextTrim();

                if (sizeStr == null) {
                    sizeStr = "";
                } else {
                    sizeStr = sizeStr.trim();
                }

                if (node.element("precision") == null) {
                    if (!sizeStr.isEmpty()) {
                        map.put(name, String.format("%s", sizeStr));
                    }

                    continue;
                }

                String precisionStr = node.element("precision").getTextTrim();

                if (precisionStr == null || precisionStr.trim().isEmpty()) {
                    map.put(name, String.format("%s", sizeStr));
                } else {
                    map.put(name, String.format("%s,%s", sizeStr, precisionStr));
                }
            }
        } catch (Exception e) {
            log.error("getDataobjectTypeColumnLenPrecision() failed!  e=" + e);
            throw e;
        }

        return map;
    }

    /*
    public static Map<String,String> getColumnLenPrecision(String fields) 
    {
        String FIELD_NODE = "//structureDataFileDefinition/column";

        Map<String,String> map = new HashMap<>();
        
        try
        {
            Document metadataXmlDoc = Tool.getXmlDocument(fields);
            List<Element> nodes = metadataXmlDoc.selectNodes(FIELD_NODE);

            for( Element node: nodes)
            {
                String name = node.element("name").getTextTrim();
                
                String sizeStr = node.element("size").getTextTrim();
 
                if ( sizeStr == null)
                    sizeStr = "";
                else
                    sizeStr = sizeStr.trim();
                
                String precisionStr = node.element("precision").getTextTrim();
 
                if ( precisionStr == null || precisionStr.trim().isEmpty() )
                    map.put(name, String.format("%s",sizeStr));
                else
                    map.put(name, String.format("%s,%s",sizeStr,precisionStr));
            }
        }
        catch(Exception e)
        {
            log.error("getPrimaryKeyFieldNames() failed!  e="+e+" fields="+fields);
            throw e;
        }
        
        return map;
    }
    
     */
    public static MetadataDataType guessDataType(String value) {
        if (value == null || value.trim().isEmpty()) {
            return MetadataDataType.STRING;
        }

        boolean isNumber = Tool.isNumber(value);

        if (isNumber && value.contains(".")) {
            if (value.length() < 8) {
                return MetadataDataType.FLOAT;
            } else {
                return MetadataDataType.DOUBLE;
            }
        }

        if (value.matches("^\\d*-\\d*-\\d*.*") || value.matches("^\\d*/\\d*/\\d*.*")) {
            if (value.length() <= 10) {
                return MetadataDataType.DATE;
            } else {
                return MetadataDataType.TIMESTAMP;
            }
        }

        return MetadataDataType.STRING;
    }

    public static Object getValueObject(String value, int dataType) {
        try {
            switch (MetadataDataType.findByValue(dataType)) {
                case STRING:
                case BINARYLINK:  // file link 
                    return value;
                case BOOLEAN:
                    return value.equals("true");
                case INTEGER:
                    try {
                        value = Tool.formalizeIntLongString(value);
                        return Integer.parseInt(value);
                    } catch (Exception e) {
                        return Integer.parseInt("0");
                    }
                case LONG:
                    try {
                        value = Tool.formalizeIntLongString(value);
                        return Long.parseLong(value);
                    } catch (Exception e) {
                        return Long.parseLong("0");
                    }
                case FLOAT:
                    try {
                        return Float.parseFloat(value);
                    } catch (Exception e) {
                        return Float.parseFloat("0.0");
                    }
                case DOUBLE:
                    try {
                        return Double.parseDouble(value);
                    } catch (Exception e) {
                        return Double.parseDouble("0.0");
                    }
                case TIMESTAMP:
                case DATE:
                    Date date = Tool.convertStringToDate(value);

                    if (date == null) {
                        date = Tool.convertStringToDate(value, "yyyy-MM-dd'T'HH:mm:ss.S");
                    }

                    return date;
            }
        } catch (Exception e) {
            log.info("111111 getValueObject() failed! e=" + e + " value=" + value + " dataType=" + dataType + " stacktrace=" + ExceptionUtils.getStackTrace(e));
        }

        return null;
    }

    public static Object getValueObject(String value, int dataType, String datetimeFormat) {
        try {
            switch (MetadataDataType.findByValue(dataType)) {
                case STRING:
                case BINARYLINK:  // file link 
                    return value;
                case BOOLEAN:
                    return value.equals("true");
                case INTEGER:
                    value = Tool.formalizeIntLongString(value);
                    return Integer.parseInt(value);
                case LONG:
                    value = Tool.formalizeIntLongString(value);
                    return Long.parseLong(value);
                case FLOAT:
                    return Float.parseFloat(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case TIMESTAMP:
                case DATE:
                    return Tool.convertStringToDate(value, datetimeFormat);
            }
        } catch (Exception e) {
            log.info(" getValueObject() failed! e=" + e);
        }

        return null;
    }

    public static Map<String, Date> getRelativeTimePeriod(int relativeTimeNumber, int relativeTimeTimeunit) {
        Date endDate = new Date();
        long seconds = TimeUnit.findByValue(relativeTimeTimeunit).getSeconds();
        long k = endDate.getTime() - seconds * 1000 * relativeTimeNumber;
        Date startDate = new Date(k);

        Map<String, Date> map = new HashMap<>();
        map.put("startDate", startDate);
        map.put("endDate", endDate);

        return map;
    }

    public static Map<String, Date> getPredefinedRelativeTimePeriod(int relativeTimeType) {
        Calendar cal = Calendar.getInstance();
        Date startDate = new Date(0);
        Date endDate = new Date();

        switch (RelativeTimeType.findByValue(relativeTimeType)) {
            case TODAY:
                endDate = new Date();
                startDate = Tool.startOfDay(endDate);
                break;
            case WEEK_TO_DATE:
            case BUSINESS_WEEK_TO_DATE:
                endDate = new Date();
                cal.setTime(endDate);

                Calendar firstDayOfThisWeek = Tool.getFirstDayOfWeek(cal);
                startDate = Tool.startOfDay(firstDayOfThisWeek.getTime());
                break;
            case MONTH_TO_DATE:
                endDate = new Date();
                cal.setTime(new Date(endDate.getTime()));
                Calendar firstDayOfThisMonth = Tool.getFirstDayOfMonth(cal);
                startDate = Tool.startOfDay(firstDayOfThisMonth.getTime());
                break;
            case YEAR_TO_DATE:
                endDate = new Date();
                cal.setTime(new Date(endDate.getTime()));
                Calendar firstDayOfThisYear = Tool.getFirstDayOfYear(cal);
                startDate = Tool.startOfDay(firstDayOfThisYear.getTime());
                break;
            case YESTERDAY:
                endDate = new Date(new Date().getTime() - 24 * 3600 * 1000);

                startDate = Tool.startOfDay(endDate);
                endDate = Tool.endOfDay(endDate);
                break;
            case PREVIOUS_WEEK:
            case PREVIOUS_BUSINESS_WEEK:
                endDate = new Date();
                cal.setTime(new Date(endDate.getTime() - 7 * 24 * 3600 * 1000));
                Calendar firstDayOfLastWeek = Tool.getFirstDayOfWeek(cal);
                Calendar lastDayOfLastWeek = Tool.getLastDayOfWeek(cal);

                startDate = Tool.startOfDay(firstDayOfLastWeek.getTime());
                endDate = Tool.endOfDay(lastDayOfLastWeek.getTime());

                break;
            case PREVIOUS_MONTH:
                endDate = new Date();
                cal.setTime(endDate);
                Calendar firstDayOfLastMonth = Tool.getFirstDayOfLastMonth(cal);
                Calendar lastDayOfLastMonth = Tool.getLastDayOfLastMonth(cal);

                startDate = Tool.startOfDay(firstDayOfLastMonth.getTime());
                endDate = Tool.endOfDay(lastDayOfLastMonth.getTime());
                break;
            case PREVIOUS_YEAR:
                endDate = new Date();
                cal.setTime(endDate);
                Calendar firstDayOfLastYear = Tool.getFirstDayOfLastYear(cal);
                Calendar lastDayOfLastYear = Tool.getLastDayOfLastYear(cal);

                startDate = Tool.startOfDay(firstDayOfLastYear.getTime());
                endDate = Tool.endOfDay(lastDayOfLastYear.getTime());
                break;
            case LAST_15_MINUTES:
                endDate = new Date();
                startDate = new Date(endDate.getTime() - 15 * 60 * 1000);
                break;
            case LAST_60_MINUTES:
                endDate = new Date();
                startDate = new Date(endDate.getTime() - 60 * 60 * 1000);
                break;
            case LAST_4_HOURS:
                endDate = new Date();
                startDate = new Date(endDate.getTime() - 4 * 3600 * 1000);
                break;
            case LAST_24_HOURS:
                endDate = new Date();
                startDate = new Date(endDate.getTime() - 24 * 3600 * 1000);
                break;
            case LAST_7_DAYS:
                endDate = new Date();
                startDate = new Date(endDate.getTime() - 7 * 24 * 3600 * 1000);
                break;
            case LAST_30_DAYS:
                endDate = new Date();
                startDate = new Date(endDate.getTime() - 15 * 24 * 3600 * 1000);   // some java bugs here  , canot minus 30 days
                startDate = new Date(startDate.getTime() - 15 * 24 * 3600 * 1000);
                break;
            default:
        }

        Map<String, Date> map = new HashMap<>();
        map.put("startDate", startDate);
        map.put("endDate", endDate);

        return map;
    }

    public static DataobjectType getDataobjectTypeByName(EntityManager platformEm, String dataobjectTypeName) throws Exception {
        DataobjectType dataobjectType = null;

        try {
            String sql = String.format("from DataobjectType where name='%s' or name='s'", dataobjectTypeName.toUpperCase().trim(), dataobjectTypeName.toLowerCase().trim());
            dataobjectType = (DataobjectType) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            throw e;
        }

        return dataobjectType;
    }

    public static DataobjectType getDataobjectTypeByIndexTypeName(EntityManager platformEm, String indexTypeName) {
        DataobjectType dataobjectType = null;

        try {
            String sql = String.format("from DataobjectType where indexTypeName='%s'", indexTypeName);
            dataobjectType = (DataobjectType) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error(" getDataobjectTypeByIndexTypeName failed! e=" + e);
            throw e;
        }

        return dataobjectType;
    }

    public static DataobjectType getDataobjectType(EntityManager platformEm, int dataobjectTypeId) {
        DataobjectType dataobjectType = null;

        try {
            dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
        } catch (Exception e) {
            log.error(" getDataobjectType failed! e=" + e);
            throw e;
        }

        return dataobjectType;
    }

    public static String getIndexTypeName(EntityManager platformEm, int dataobjectTypeId) {
        DataobjectType dataobjectType = null;

        try {
            dataobjectType = platformEm.find(DataobjectType.class, dataobjectTypeId);
        } catch (Exception e) {
            log.error(" getIndexTypeName failed! e=" + e);
            throw e;
        }

        return dataobjectType.getIndexTypeName();
    }

    public static String getIndexSetName(int organizationId, int repositoryId, String metadataName, String metadataValue) {
        String indexName = String.format("%s_%d_%d_%s_%s_alias", CommonKeys.DATA_STORE_NAME, organizationId, repositoryId, metadataName, metadataValue);
        return indexName.replaceAll("[\\\\/|*?<>\", ]", "-").toLowerCase();  // index name cannot include those character
    }

    public static String getDataMartIndexSetName(int organizationId, int repositoryId, String metadataName, String metadataValue) {
        String indexName = String.format("%s_%d_%d_%s_%s_alias", CommonKeys.DATA_MART_NAME, organizationId, repositoryId, metadataName, metadataValue);
        return indexName.replaceAll("[\\\\/|*?<>\", ]", "-").toLowerCase();  // index name cannot include those character
    }

    public static String getIndexName(int organizationId, int repositoryId, String metadataName) {
        String indexName = String.format("%s_%d_%d_%s_*", CommonKeys.DATA_STORE_NAME, organizationId, repositoryId, metadataName);
        return indexName.replaceAll("[\\\\/|?<>\", ]", "-").toLowerCase();  // index name cannot include those character
    }

    public static LogFileDefinition getLogFileDefinition(EntityManager platformEm, String name) {
        LogFileDefinition logFileDefinition = null;

        try {
            String sql = String.format("from LogFileDefinition lfd where lfd.name='%s'", name);
            logFileDefinition = (LogFileDefinition) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error(" getLogFileDefinition failed! e=" + e);
            throw e;
        }

        return logFileDefinition;
    }

    public static int getMetadataDataType(String metadataNameInput, List<Map<String, Object>> metadataDefinitions) {
        for (Map<String, Object> definition : metadataDefinitions) {
            String metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
            String dataType = (String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);

            if (metadataName.equals(metadataNameInput)) {
                return Integer.parseInt(dataType);
            }
        }

        return 0;
    }

    public static String changeDataviewColumnName(String columnName) {
        String newColumnName = columnName.replace("(*)", "(all)");
        newColumnName = newColumnName.replace("(", "_");

        return newColumnName.replace(")", "");
    }

    public static String getDataobjectCheckSum(Map<String, Object> jsonMap) {
        StringBuilder builder = new StringBuilder();

        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value != null) {
                builder.append(key).append(value.toString());
            }
        }

        MessageDigest sha = null;

        try {
            sha = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException ex) {
            log.error("getDataobjectCheckSum failed! e=" + ex);
            return "";
        }

        sha.update(builder.toString().getBytes());

        return FileUtil.byteArrayToHex(sha.digest());
    }

    public static String getTaskCronExpression(int scheduledRunType, int scheduleNum, Date scheduleTime) {
        int second = 0, minute = 0, hour = 0, day = 0, month = 0, year = 0;
        String cronExpression = "";
        Calendar cal = Calendar.getInstance();

        if (ScheduledRunType.findByValue(scheduledRunType) != ScheduledRunType.RUN_PER_HOUR) {
            cal.setTime(scheduleTime);

            second = cal.get(Calendar.SECOND);
            minute = cal.get(Calendar.MINUTE);
            hour = cal.get(Calendar.HOUR_OF_DAY);
            day = cal.get(Calendar.DAY_OF_MONTH);
            month = cal.get(Calendar.MONTH) + 1;
            year = cal.get(Calendar.YEAR);
        }

        switch (ScheduledRunType.findByValue(scheduledRunType)) {
            case RUN_PER_HOUR:
                cronExpression = String.format("0 %d * * * ?", scheduleNum);
                break;
            case RUN_PER_DAY:
                cronExpression = String.format("%d %d %d * * ?", second, minute, hour);
                break;
            case RUN_PER_WEEK:
                cronExpression = String.format("%d %d %d ? * %s", second, minute, hour, WeekDays.findByValue(scheduleNum).getShortName());
                break;
            case RUN_PER_MONTH:
                cronExpression = String.format("%d %d %d %d * ?", second, minute, hour, scheduleNum);
                break;
            case RUN_ONCE:
                cronExpression = String.format("%d %d %d %d %d ? %d", second, minute, hour, day, month, year);
                break;
            case RUN_PER_X_SECONDS:
                cronExpression = String.format("0/%d * * * * ?", scheduleNum);
                break;
            case RUN_PER_X_MINUTES:
                cronExpression = String.format("0 0/%d * * * ?", scheduleNum);
                break;
            case RUN_PER_X_HOURS:
                cronExpression = String.format("0 0 0/%d * * ?", scheduleNum);
                break;
        }

        return cronExpression;
    }

    public static List<DataobjectVO> generateDataobjectsForDataset(List<Map<String, String>> searchResults, boolean needEncryption) throws Exception {
        List<DataobjectVO> dataobjectVOs = new ArrayList<>();
        DataobjectVO dataobjectVO;
        Dataobject dataobject;
        JSONObject resultFields;
        String id;

        if (searchResults == null) {
            return dataobjectVOs;
        }

        for (Map<String, String> searchResult : searchResults) {
            try {
                resultFields = new JSONObject(searchResult.get("fields"));

                dataobject = new Dataobject();
                dataobjectVO = new DataobjectVO();

                id = searchResult.get("id");
                dataobjectVO.setId(id.substring(0, 40));
                dataobjectVO.setDataobject(dataobject);

                dataobject.setId(id.substring(0, 40)); // dataobject id
                dataobject.setCurrentVersion(Integer.parseInt(id.substring(40))); // dataobject version
                dataobject.setOrganizationId(resultFields.optInt("organization_id"));
                dataobject.setDataobjectType(resultFields.optInt("dataobject_type"));
                dataobject.setRepositoryId(resultFields.optInt("target_repository_id"));

                dataobjectVO.setThisDataobjectTypeMetadatas(Tool.parseJson(resultFields));

                //log.info("last_stored_time="+resultFields.optString("last_stored_time"));
                dataobjectVOs.add(dataobjectVO);
            } catch (Exception e) {
                log.error("generateDataobjectsForDataset() failed!  id = " + e);
                throw e;
            }
        }

        return dataobjectVOs;
    }

    public static List<DataobjectVO> generateDataobjectsFromPreCalculation(List<Map<String, String>> preCalculationResults) throws Exception {
        List<DataobjectVO> dataobjectVOs = new ArrayList<>();
        DataobjectVO dataobjectVO;
        Dataobject dataobject;
        String id;

        if (preCalculationResults == null) {
            return dataobjectVOs;
        }

        for (Map<String, String> result : preCalculationResults) {
            try {
                dataobject = new Dataobject();
                dataobjectVO = new DataobjectVO();

                dataobjectVO.setDataobject(dataobject);
                dataobjectVO.setThisDataobjectTypeMetadatas(result);

                dataobjectVOs.add(dataobjectVO);
            } catch (Exception e) {
                log.error("ggenerateDataobjectsFromPreCalculation() failed!  id = " + e);
                throw e;
            }
        }

        return dataobjectVOs;
    }

    public static List<DataobjectVO> generateDataobjectsForDatasetAggregation(List<Map<String, String>> searchResults) {
        List<DataobjectVO> dataobjectVOs = new ArrayList<>();
        DataobjectVO dataobjectVO;
        Dataobject dataobject;

        int i = 0;

        for (Map<String, String> searchResult : searchResults) {
            try {
                dataobject = new Dataobject();
                dataobjectVO = new DataobjectVO();

                dataobjectVO.setId(String.valueOf(i));
                i++;

                dataobjectVO.setDataobject(dataobject);

                Map<String, String> newData = new HashMap<>();

                for (Map.Entry<String, String> entry : searchResult.entrySet()) {
                    String val = entry.getValue();

                    if (val != null) {
                        val = val.trim();

                        if (!val.isEmpty() && Tool.isNumber(val)) {
                            val = Tool.normalizeNumber(val);
                        }
                    }

                    newData.put(entry.getKey(), val);
                }

                dataobjectVO.setThisDataobjectTypeMetadatas(newData);

                dataobjectVOs.add(dataobjectVO);
            } catch (Exception ex) {
                log.error("generateDataobjectsForDataset()Aggregation failed!  id = " + ex);
                ex.printStackTrace();
            }
        }

        return dataobjectVOs;
    }

    public static DatasourceConnection getDatasourceConnectionById(EntityManager em, int organizationId, int id) {
        DatasourceConnection datasourceConnection = null;

        try {
            String sql = String.format("from DatasourceConnection where organizationId=%d and id=%d", organizationId, id);
            datasourceConnection = (DatasourceConnection) em.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error("failed to getDatasourceConnection! e=" + e + " dcname=" + id);
            return null;
        }

        return datasourceConnection;
    }

    public static DatasourceConnection getDatasourceConnectionByDatasourceId(EntityManager em, int organizationId, int datasourceId) {
        DatasourceConnection datasourceConnection = null;

        try {
            String sql = String.format("from Datasource where organizationId=%d and id=%d", organizationId, datasourceId);
            Datasource datasource = (Datasource) em.createQuery(sql).getSingleResult();
            datasourceConnection = em.find(DatasourceConnection.class, datasource.getDatasourceConnectionId());
        } catch (Exception e) {
            log.error("failed to getDatasourceConnectionByDatasourceId()! e=" + e + " datasourceId=" + datasourceId);
            throw e;
        }

        return datasourceConnection;
    }

    public static DatasourceConnection getDatasourceConnectionByName(EntityManager em, int organizationId, String datasourceConnectionName) {
        DatasourceConnection datasourceConnection = null;

        try {
            String sql = String.format("from DatasourceConnection dc where dc.organizationId=%d and dc.name='%s'", organizationId, datasourceConnectionName);
            datasourceConnection = (DatasourceConnection) em.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error("failed to getDatasourceConnection! e=" + e + " dcname=" + datasourceConnectionName);
            return null;
        }

        return datasourceConnection;
    }

    public static ByteBuffer getContentFromDatabase(EntityManager em, EntityManager platformEm, String contentI, String contentLocation) throws Exception {
        throw new PlatformException("platformException.error.0001");
    }

    public static ByteBuffer getContentFromDatabase(EntityManager em, EntityManager platformEm, String dataobjectId, int dataobjectVersion, String contentLocation) throws Exception {
        throw new PlatformException("platformException.error.0001");
    }

    public static boolean checkIfDataobjectTypeHasContent(List<Map<String, Object>> metadataInfoList) {
        for (Map<String, Object> metadataInfo : metadataInfoList) {
            if (Integer.parseInt((String) metadataInfo.get(CommonKeys.METADATA_NODE_DATA_TYPE)) == MetadataDataType.BINARY.getValue()
                    || Integer.parseInt((String) metadataInfo.get(CommonKeys.METADATA_NODE_DATA_TYPE)) == MetadataDataType.BINARYLINK.getValue()) {
                return true;
            }
        }

        return false;
    }

    public static void generateDocumentIdAndDatasource(List<DiscoveredDataobjectInfo> dataInfoList, Job job, int datasourceId) {
        String fullPathname;

        for (DiscoveredDataobjectInfo dataInfo : dataInfoList) {
            if (dataInfo.getPath() == null || dataInfo.getPath().trim().isEmpty()) {
                fullPathname = dataInfo.getName();
            } else {
                fullPathname = dataInfo.getPath() + "/" + dataInfo.getName();
            }

            String dataobjectId = DataIdentifier.generateDataobjectId(job.getOrganizationId(), job.getSourceApplicationId(), job.getDatasourceType(), datasourceId, fullPathname, job.getTargetRepositoryId());

            dataInfo.setDataobjectId(dataobjectId);
            dataInfo.setDatasourceId(datasourceId);
        }
    }

    public static String mountDatasourceConnectionFolder(DatasourceConnection datasourceConnection) {
        String fileserverType = datasourceConnection.getSubType();

        String localMappingPath = String.format("/mnt/ef/datasource-connection-%d", datasourceConnection.getId());

        // mkdir localMappingPath
        String commandStr = String.format("mkdir %s", localMappingPath);
        RCUtil.executeCommand(commandStr, null, false, null, null, false);  //file_mode=0777,dir_mode=0777

        // mount localMappingPath
        if (datasourceConnection.getUserName() == null || datasourceConnection.getUserName().trim().isEmpty()) {
            commandStr = String.format("sudo mount -t %s %s %s -o guest,rw%s", fileserverType, datasourceConnection.getLocation(), localMappingPath, mountStr);
        } else {
            commandStr = String.format("sudo mount -t %s %s %s -o username=%s,password=%s%s", fileserverType, datasourceConnection.getLocation(), localMappingPath, datasourceConnection.getUserName(), datasourceConnection.getPassword(), mountStr);
        }

        RCUtil.executeCommand(commandStr, null, false, null, null, false);

        return localMappingPath;
    }

    public static String mountFileServerToLocalFolder(DatasourceConnection datasourceConnection, Datasource datasource, Date businessDate) {
        String fileserverType = datasourceConnection.getSubType();
        String subFolder = getDatasourceSubFolder(datasource);

        subFolder = Tool.convertIfItIsTimeFolder(subFolder, businessDate);

        String localMappingPath = String.format("%s/datasource-connection-%d", CommonKeys.MOUNT_POINT, datasourceConnection.getId());

        // mkdir localMappingPath
        String commandStr = String.format("mkdir %s", localMappingPath);
        RCUtil.executeCommand(commandStr, null, false, null, null, false);

        // mount localMappingPath
        if (datasourceConnection.getUserName().length() > 0) {
            commandStr = String.format("sudo mount -t %s %s %s -o username=%s,password=%s%s", fileserverType, datasourceConnection.getLocation(), localMappingPath, datasourceConnection.getUserName(), datasourceConnection.getPassword(), mountStr);
        } else {
            commandStr = String.format("sudo mount -t %s %s %s -o guest,rw%s", fileserverType, datasourceConnection.getLocation(), localMappingPath, mountStr);
        }

        RCUtil.executeCommand(commandStr, null, false, null, null, false);

        return Tool.convertToNonWindowsPathFormat(localMappingPath + subFolder);
    }

    public static String mountLocationToLocalFolder(String fileserverType, String location, String userName, String password) {
        String commandStr = String.format("mkdir %s", CommonKeys.MOUNT_POINT);
        RCUtil.executeCommand(commandStr, null, false, null, null, false);

        // mkdir localMappingPath
        String localMappingPath = String.format("%s/installation", CommonKeys.MOUNT_POINT);

        commandStr = String.format("mkdir %s", localMappingPath);
        RCUtil.executeCommand(commandStr, null, false, null, null, false);

        // mount localMappingPath
        if (userName.length() > 0) {
            commandStr = String.format("sudo mount -t %s %s %s -o username=%s,password=%s%s", fileserverType, location, localMappingPath, userName, password, mountStr);
        } else {
            commandStr = String.format("sudo mount -t %s %s %s -o guest,rw%s", fileserverType, location, localMappingPath, mountStr);
        }

        RCUtil.executeCommand(commandStr, null, false, null, null, false);

        return Tool.convertToNonWindowsPathFormat(localMappingPath);
    }

    public static String getOriginalSourcePath(DatasourceConnection datasourceConnection, Datasource datasource, String archiveFile) {
        String localMappingPath = String.format("%s/datasource-connection-%d%s/", CommonKeys.MOUNT_POINT, datasourceConnection.getId(), getDatasourceSubFolder(datasource));

        String filepath = archiveFile.substring(localMappingPath.length());

        String subFolder = getSubFolder(datasourceConnection, datasource, null);

        return String.format("%s/%s", subFolder, filepath);
    }

    public static String getDatabaseSchema(DatasourceConnection datasourceConnection) {
        String schema = Util.getDatasourceConnectionProperty(datasourceConnection, "schema");

        if (schema == null || schema.trim().isEmpty()) {
            return "";
        } else {
            return schema.trim() + ".";
        }
    }

    public static String getDatasourceConnectionProperty(DatasourceConnection datasourceConnection, String propertyName) {
        String value = null;

        Document propertiesXml = Tool.getXmlDocument(datasourceConnection.getProperties());

        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_CONNECTION_PROPERTIES);

        for (Element element : list) {
            Element nameElement = element.element("name");

            if (nameElement.getTextTrim().equals(propertyName)) {
                value = element.element("value").getTextTrim();
            }
        }

        return value;
    }

    public static String getDatasourceProperty(Datasource datasource, String propertyName) {
        String value = null;

        Document propertiesXml = Tool.getXmlDocument(datasource.getProperties());

        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_PROPERTIES);

        for (Element element : list) {
            Element nameElement = element.element("name");

            if (nameElement.getTextTrim().equals(propertyName)) {
                value = element.element("value").getTextTrim();
            }
        }

        return value;
    }

    public static String getSubFolder(DatasourceConnection datasourceConnection, Datasource datasource, Date businessDate) {
        String datasourcePath = "";

        Document propertiesXml = Tool.getXmlDocument(datasource.getProperties());

        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_PROPERTIES);

        for (Element element : list) // /$(yyyy-MM:this_month)
        {
            Element nameElement = element.element("name");

            if (nameElement.getTextTrim().equals("sub_folder")) {
                datasourcePath = element.element("value").getTextTrim();
            }
        }

        if (!datasourcePath.startsWith("\\") && !datasourcePath.startsWith("/")) {
            datasourcePath = "/" + datasourcePath;
        }

        datasourcePath = Tool.convertIfItIsTimeFolder(datasourcePath, businessDate);

        return String.format("%s%s", datasourceConnection.getLocation(), datasourcePath);
    }

    public static String getDatasourceBusinessDate(Datasource datasource) {
        String businessDateStr = "";
        String datasourcePath = "";

        try {
            Document propertiesXml = Tool.getXmlDocument(datasource.getProperties());

            List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_PROPERTIES);

            for (Element element : list) // /$(yyyy-MM:this_month)
            {
                Element nameElement = element.element("name");

                if (nameElement.getTextTrim().equals("sub_folder")) {
                    datasourcePath = element.element("value").getTextTrim();
                }
            }

            if (datasourcePath.contains("$(") && datasourcePath.endsWith(")")) {
                String timeFolderTypeStr = datasourcePath.substring(datasourcePath.indexOf(":") + 1, datasourcePath.indexOf(")"));
                Date date;

                TimeFolderType type = TimeFolderType.findByName(timeFolderTypeStr);

                if (type == TimeFolderType.YESTERDAY) {
                    date = Tool.dateAddDiff(new Date(), -24, Calendar.HOUR);
                } else {
                    date = new Date();
                }

                businessDateStr = Tool.convertDateToString(date, type.getFormat());
            }
        } catch (Exception e) {
            log.error(" getDatasourceBusinessDate() failed!  e=" + e);
        }

        return businessDateStr;
    }

    public static String getDatasourceSubFolder(Datasource datasource) {
        String datasourcePath = "";

        Document propertiesXml = Tool.getXmlDocument(datasource.getProperties());

        List<Element> list = propertiesXml.selectNodes(CommonKeys.DATASOURCE_PROPERTIES);

        for (Element element : list) {
            Element nameElement = element.element("name");

            if (nameElement.getTextTrim().equals("sub_folder")) {
                datasourcePath = element.element("value").getTextTrim();
            }
        }

        if (!datasourcePath.startsWith("\\") && !datasourcePath.startsWith("/")) {
            datasourcePath = "/" + datasourcePath;
        }

        return datasourcePath;
    }

    public static List<String> getConfigData(String config, String nodeName) {
        Document taskConfigXml = Tool.getXmlDocument(config);
        List<Element> list = taskConfigXml.selectNodes(nodeName);

        List<String> configData = new ArrayList<>();
        for (Element element : list) {
            configData.add(element.getText());
        }

        return configData;
    }

    public static String getDataobjectTypeName(EntityManager platformEm, int dataobjectTypeId) throws PlatformException, Exception {
        String name = "";

        try {
            String sql = String.format("select d.name from DataobjectType d where d.id=%d", dataobjectTypeId);
            name = (String) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
        }

        return name;
    }

    public static String getContentTextFromBinary(ContentType contentType, ByteBuffer contentBinary, String encoding) {
        String contentText = "";

        if (contentType == ContentType.PDF || contentType == ContentType.HTML || contentType == ContentType.OTHERS
                || contentType == ContentType.WORD || contentType == ContentType.PPT || contentType == ContentType.EXCEL) {
            contentText = CAUtil.extractText(contentBinary);

            if (encoding.length() > 0 && contentText.length() == 0) {
                try {
                    contentText = Tool.getStringFromByteBuffer(contentBinary, encoding);
                } catch (Exception e) {
                    log.error(" convert to string failed! encoding =" + encoding + " e+" + e.getMessage());
                }
            }
        } else if (contentType == ContentType.TEXT) {
            try {
                contentText = Tool.getStringFromByteBuffer(contentBinary, encoding);
            } catch (Exception e) {
                log.error(" convert to string failed! encoding =" + encoding + " e+" + e.getMessage());
            }
        }

        return contentText;
    }

    public static String getContentTextFromBinary(ByteBuffer contentBinary, boolean isText, String encoding) {
        String contentText = "";

        if (isText == false) {
            contentText = CAUtil.extractText(contentBinary);

            if (encoding.length() > 0 && contentText.length() == 0) {
                try {
                    contentText = Tool.getStringFromByteBuffer(contentBinary, encoding);
                } catch (Exception e) {
                    log.error(" convert to string failed! encoding =" + encoding + " e+" + e.getMessage());
                }
            }
        } else {
            try {
                contentText = Tool.getStringFromByteBuffer(contentBinary, encoding);
            } catch (Exception e) {
                log.error(" convert to string failed! encoding =" + encoding + " e+" + e.getMessage());
            }
        }

        return contentText;
    }

    public static String getOrganizationMQIP(EntityManager platformEm, int organizationId) {
        String sql = String.format("select si.location from ServiceInstance si where si.id in "
                + "( select osi.serviceInstanceId from OrganizationServiceInstance osi where osi.organizationServiceInstancePK.organizationId=%d "
                + "and osi.organizationServiceInstancePK.serviceInstanceType=%d)", organizationId, PlatformServiceType.MQ_SERVICE.getValue());

        String IP = (String) platformEm.createQuery(sql).getSingleResult();

        return IP;
    }

    public static String getRepositorySearchEngineClusterName(EntityManager em, EntityManager platformEm, int repositoryId) {
        String sql = String.format("select r.searchEngineServiceInstanceId from Repository r where r.id = %d", repositoryId);
        int searchEngineServiceInstanceId = (Integer) em.createQuery(sql).getSingleResult();

        sql = String.format("select si.location from ServiceInstance si where si.id = %d", searchEngineServiceInstanceId);
        String clusterName = (String) platformEm.createQuery(sql).getSingleResult();

        return clusterName;
    }

    public static ServiceInstance getRepositorySearchEngineServiceInstance(EntityManager em, EntityManager platformEm, int repositoryId) {
        String sql = String.format("select r.searchEngineServiceInstanceId from Repository r where r.id = %d", repositoryId);
        log.info(" sql=" + sql);
        int searchEngineServiceInstanceId = (Integer) em.createQuery(sql).getSingleResult();

        sql = String.format("from ServiceInstance si where si.id = %d", searchEngineServiceInstanceId);
        ServiceInstance serviceInstance = (ServiceInstance) platformEm.createQuery(sql).getSingleResult();

        return serviceInstance;
    }

    public static List<String> getRepositorySearchEngineIP(EntityManager em, EntityManager platformEm, int repositoryId) {
        String sql = String.format("select r.searchEngineServiceInstanceId from Repository r where r.id = %d", repositoryId);
        int searchEngineServiceInstanceId = (Integer) em.createQuery(sql).getSingleResult();

        sql = String.format("select cn.internalIP from ComputingNode cn where cn.id in ( select sicn.serviceInstanceComputingNodePK.computingNodeId from ServiceInstanceComputingNode sicn where sicn.serviceInstanceComputingNodePK.serviceInstanceId = %d)", searchEngineServiceInstanceId);
        List<String> IPs = platformEm.createQuery(sql).getResultList();

        return IPs;
    }

    public static void sortListAccordingToScore(List<Map<String, Object>> list) {
        Collections.sort(list, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> content1, Map<String, Object> content2) {
                float score1 = Float.parseFloat((String) content1.get("score"));
                float score2 = Float.parseFloat((String) content2.get("score"));

                if (score1 < score2) {
                    return 1;
                } else if (score1 > score2) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
    }

    public static String getSystemConfigValue(EntityManager platformEm, int organizationId, String key, String name) {
        String value = "";

        try {
            String sql = String.format("select sc.value from SystemConfig sc where sc.organizationId=%d and sc.configKey='%s' and sc.name = '%s'", organizationId, key, name);
            value = (String) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            //log.error(" getSystemConfigValue() failed! e="+e.getMessage()+" key="+key+" name="+name);
        }

        return value;
    }

    public static List<String> getSystemConfigValueList(EntityManager platformEm, int organizationId, String key) throws PlatformException, Exception {
        List<String> values = null;

        try {
            String sql = String.format("select sc.value from SystemConfig sc where sc.organizationId=%d and sc.key='%s'", organizationId, key);
            values = (List<String>) platformEm.createQuery(sql).getResultList();
        } catch (Exception e) {
            log.error(" getSystemConfigValueList() failed! e=" + e.getMessage());
            throw e;
        }

        return values;
    }

    public static int getServiceGuardPort(String nodeType) {
        //int port = CommonKeys.THRIFT_SERVICE_GUARD_PORT;
        //int port = 0;

        return CommonKeys.THRIFT_SERVICE_GUARD_PORT;
        /*
        if ( nodeType.equals("service_node") )
            port = CommonKeys.THRIFT_SERVICE_GUARD_PORT_FOR_SERVICE_NODE;
        else
        if ( nodeType.equals("management_node") )
            port = CommonKeys.THRIFT_SERVICE_GUARD_PORT_FOR_MANAGEMENT_NODE;
        else
        if ( nodeType.equals("data_collecting_node") )
            port = CommonKeys.THRIFT_SERVICE_GUARD_PORT_FOR_DATA_COLLECTING_NODE;
        
        return port;*/
    }

    public static int convertStringTypeToJdbcType(String sqlDataTypeStr) throws Exception {
        if (sqlDataTypeStr == null || sqlDataTypeStr.trim().isEmpty()) {
            return 0;
        }

        sqlDataTypeStr = sqlDataTypeStr.trim().toLowerCase();

        if (sqlDataTypeStr.startsWith("string") || sqlDataTypeStr.startsWith("varchar") || sqlDataTypeStr.startsWith("char")) {
            return 1;
        } else if (sqlDataTypeStr.startsWith("int") || sqlDataTypeStr.startsWith("tinyint") || sqlDataTypeStr.startsWith("smallint")) {
            return 4;
        } else if (sqlDataTypeStr.startsWith("bigint")) {
            return -5;
        } else if (sqlDataTypeStr.startsWith("float")) {
            return 6;
        } else if (sqlDataTypeStr.startsWith("double") || sqlDataTypeStr.startsWith("decimal")) {
            return 2;
        } else if (sqlDataTypeStr.startsWith("timestamp")) {
            return 92;
        } else if (sqlDataTypeStr.startsWith("date")) {
            return 91;
        }

        return 0;
    }

    public static int convertToSystemDataType(String sqlDataTypeStr, String lenStr, String precisionStr) throws Exception {
        int systemDataType;
        int len;
        int precision;

        int sqlDataType = Integer.parseInt(sqlDataTypeStr);

        switch (sqlDataType) {
            case 4:
            case 5:
            case -6:
            case -7:
                systemDataType = MetadataDataType.INTEGER.getValue();
                break;
            case -5:
                systemDataType = MetadataDataType.LONG.getValue();
                break;
            case 6:
            case 7:
                systemDataType = MetadataDataType.FLOAT.getValue();
                break;
            case 2:
            case 3:
            case 8:

                if (precisionStr == null) {
                    systemDataType = MetadataDataType.DOUBLE.getValue();
                    break;
                }

                try {
                    len = Integer.parseInt(lenStr);
                } catch (Exception e) {
                    len = 0;
                }

                try {
                    precision = Integer.parseInt(precisionStr);
                } catch (Exception e) {
                    precision = 0;
                }
                //log.info(" 111111 len="+len+" precision="+precision);

                if (precision > 0) {
                    if (len > 8) {
                        systemDataType = MetadataDataType.DOUBLE.getValue();
                    } else {
                        systemDataType = MetadataDataType.FLOAT.getValue();
                    }
                } else if (len >= 18) {
                    systemDataType = MetadataDataType.DOUBLE.getValue();
                } else if (len > 8) {
                    systemDataType = MetadataDataType.LONG.getValue();
                } else {
                    systemDataType = MetadataDataType.INTEGER.getValue();
                }

                if (len == 0 && precision <= 0) {
                    systemDataType = MetadataDataType.DOUBLE.getValue();
                }

                break;
            case -15:
            case -9:
            case 1:
            case 9:
            case 12:
            case 1111:
                systemDataType = MetadataDataType.STRING.getValue();
                break;
            case 91:
                systemDataType = MetadataDataType.DATE.getValue();
                break;
            case 92:
            case 93:
                systemDataType = MetadataDataType.TIMESTAMP.getValue();
                break;
            case -1:
            case -2:
            case -3:
            case -4:
            case -16:
            case 2004:
                systemDataType = MetadataDataType.BINARY.getValue();
                break;
            case 2005:
                systemDataType = MetadataDataType.STRING.getValue();
                break;
            case 16:
                systemDataType = MetadataDataType.BOOLEAN.getValue();
                break;
            default:
                throw new Exception("type not found! sqlDataType=" + sqlDataType);
        }

        //log.info(" 111111 sqlDataTypeStr="+sqlDataTypeStr+" len="+lenStr+" precisionStr="+precisionStr+" edfDataType="+systemDataType+" "+MetadataDataType.findByValue(systemDataType).name());
        return systemDataType;
    }

    public static String getNameByIdInEnum(String enumName, int id) {
        try {
            Class<?> enumClass = Class.forName("com.broaddata.common.model.enumeration." + enumName);

            for (Object obj : enumClass.getEnumConstants()) {
                Enum e = (Enum) obj;

                Class<?> eClass = e.getClass();
                Method mth = eClass.getDeclaredMethod("getValue");
                int val = (int) mth.invoke(e);

                if (val == id) {
                    return e.toString();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }

        return "";
    }

    public static void DeleteDataWorkerAdminData() throws Exception {
        try {
            String filename = String.format("%s/DataWorkerShareData", System.getProperty("user.dir"));
            FileUtil.delFile(filename);
        } catch (Exception e) {
            log.error("Failed to DeleteDataWorkerAdminData()! e=" + e.getMessage());
        }
    }

    public static DataWorkerAdminData getDataWorkerAdminData() throws Exception {
        DataWorkerAdminData data = new DataWorkerAdminData();

        try {
            String filename = String.format("%s/DataWorkerShareData", System.getProperty("user.dir"));
            byte[] fileStream = FileUtil.readFileToByteArray(new File(filename));
            data = (DataWorkerAdminData) Tool.deserializeObject(fileStream);
        } catch (Exception e) {
            log.error("Failed to getDataWorkerAdminData!  e=" + e.getMessage());
        }

        return data;
    }

    public static DatasourceType getDatasourceType(int datasourceType, List<DatasourceType> datasourceTypes) {
        for (DatasourceType type : datasourceTypes) {
            if (datasourceType == type.getId()) {
                return type;
            }
        }
        return null;
    }

    public static byte[] getDataobjectContent(int storageServiceInstanceId, String contentId) {
        byte[] content = null;

        return content;
    }

    public static String findSingleValueMetadataValue(String name, List<FrontendMetadata> metadataList) {
        if (name == null || metadataList == null) {
            return "";
        }

        for (int i = 0; i < metadataList.size(); i++) {
            if (name.trim().toLowerCase().equals(metadataList.get(i).getName().trim().toLowerCase())) {
                return metadataList.get(i).getSingleValue();
            }
        }

        return "";
    }

    public static FrontendMetadata findSingleValueMetadata(String name, List<FrontendMetadata> metadataList) {
        for (int i = 0; i < metadataList.size(); i++) {
            if (name.equals(metadataList.get(i).getName())) {
                return metadataList.get(i);
            }
        }

        return null;
    }

    public static String findSingleValueContentMetadataValue(String contentId, String name, List<FrontendMetadata> metadataList) {
        for (int i = 0; i < metadataList.size(); i++) {
            if (name.equals(metadataList.get(i).getName()) && contentId.equals(metadataList.get(i).getContentId())) {
                return metadataList.get(i).getSingleValue();
            }
        }

        return "";
    }

    public static List<String> findMutiValueMetadataValue(String name, List<FrontendMetadata> metadataList) {
        List<String> list = new ArrayList();

        for (int i = 0; i < metadataList.size(); i++) {
            if (name.equals(metadataList.get(i).getName())) {
                list = metadataList.get(i).getMultiValues();
            }
        }

        return list;
    }

    public static List<String> findMutiValueContentMetadataValue(String dataobjectId, int version, String contentId, String name, List<FrontendMetadata> metadataList) {
        List<String> list = new ArrayList();

        for (int i = 0; i < metadataList.size(); i++) {
            if (name.equals(metadataList.get(i).getName()) && contentId.equals(metadataList.get(i).getContentId())) {
                list = metadataList.get(i).getMultiValues();
            }
        }
        return list;
    }

    /*
    public static List<String> findMutiValueMetadataValue(String dataobjectId, int version, String name, List<Metadata> metadatas) 
    {
        List<String> list = new ArrayList();

        for(int i=0;i<metadatas.size();i++)
        {
            if ( name.equals( metadatas.get(i).getMetadataPK().getMetadataName() ) &&
                    dataobjectId.equals( metadatas.get(i).getMetadataPK().getDataobjectId()) &&             
                    metadatas.get(i).getMetadataPK().getVersion()==version )
                
                list.add(metadatas.get(i).getValue());
        }
        return list;
    } 
        
    public static List<String> findMutiValueContentMetadataValue(String dataobjectId, int version, String contentId, String name, List<Metadata> metadatas) 
    {
        List<String> list = new ArrayList();

        for(int i=0;i<metadatas.size();i++)
        {
            if ( name.equals( metadatas.get(i).getMetadataPK().getMetadataName() ) &&
                    dataobjectId.equals( metadatas.get(i).getMetadataPK().getDataobjectId()) &&
                    contentId.equals(metadatas.get(i).getMetadataPK().getContentId()) &&                    
                    metadatas.get(i).getMetadataPK().getVersion()==version )
                
                list.add(metadatas.get(i).getValue());
        }
        return list;
    } 
     */
    public static DataobjectType getDataobjectType1(int dataobjectTypeId) {
        if (RuntimeContext.dataobjectTypes == null) {
            RuntimeContext.dataobjectTypes = getDataobjectTypes();
        }

        for (DataobjectType obj : RuntimeContext.dataobjectTypes) {
            if (obj.getId().intValue() == dataobjectTypeId) {
                return obj;
            }
        }
        return null;
    }

    public static List<DatasourceType> getDatasourceTypesFromClient(CommonService.Client commonService) {
        List<DatasourceType> datasourceTypes = new ArrayList();

        try {
            List<ByteBuffer> bufList = commonService.getDatasourceTypes();

            for (ByteBuffer buf : bufList) {
                datasourceTypes.add((DatasourceType) Tool.deserializeObject(buf.array()));
            }
        } catch (Exception e) {
            log.error("failed to getDatasourceTypesFromClient()! e=", e);
            return null;
        }
        return datasourceTypes;
    }

    public static List<DataobjectType> getDataobjectTypesFromClient(CommonService.Client commonService) {
        List<DataobjectType> dataobjectTypes = new ArrayList();

        try {
            List<ByteBuffer> bufList = commonService.getDataobjectTypes();

            for (ByteBuffer buf : bufList) {
                dataobjectTypes.add((DataobjectType) Tool.deserializeObject(buf.array()));
            }
        } catch (Exception e) {
            log.error("failed to getDatasourceTypesFromClient()! e=", e);
            return null;
        }
        return dataobjectTypes;
    }

    public static void resetEntityManagerFactory(int organizationId) {
        log.error("reset EntityManagerFactory! organizationId=" + organizationId);
        emfList.put(organizationId, null);
    }

    public static EntityManagerFactory getEntityManagerFactory(int organizationId) {
        //log.info(" getEntityManagerFactory() ...");

        EntityManagerFactory emf = emfList.get(organizationId);

        if (emf == null) {
            lockForEm.lock();// 取得锁

            emf = emfList.get(organizationId);

            if (emf == null) {
                try {
                    EntityManager em = getPlatformEntityManagerFactory().createEntityManager();

                    String sql = String.format("from ServiceInstance si where si.type=%d and si.id in (select osi.serviceInstanceId from OrganizationServiceInstance osi where osi.organizationServiceInstancePK.organizationId = %d)",
                            PlatformServiceType.EDF_ORGANIZATION_DATABASE_SERVICE.getValue(), organizationId);

                    ServiceInstance dbServiceInstance = (ServiceInstance) em.createQuery(sql).getSingleResult();
                    Map<String, String> properties = getServiceInstancePropertyConfigMap(dbServiceInstance.getConfig());

                    Map<String, Object> persistenceMap = new HashMap<>();

                    persistenceMap.put("hibernate.connection.url", properties.get("url"));
                    persistenceMap.put("hibernate.connection.username", properties.get("username"));
                    persistenceMap.put("hibernate.connection.password", properties.get("password"));
                    // persistenceMap.put("javax.persistence.lock.timeout", CommonKeys.JPA_LOCK_TIMEOUT); // 1 second

                    log.info(String.format("Connecting TO db %s: url=%s,username=%s,password=%s", CommonKeys.ORGANIZATION_DATABASE_NAME, properties.get("url"), properties.get("username"), properties.get("password")));

                    emf = Persistence.createEntityManagerFactory(CommonKeys.ORGANIZATION_DATABASE_NAME, persistenceMap);
                    emfList.put(organizationId, emf);
                } catch (Exception e) {
                    log.error("failed to getEntityManagerFactory! e=" + e.getMessage());
                    emfList.put(organizationId, null);
                    throw e;
                } finally {
                    lockForEm.unlock();
                }
            } else {
                lockForEm.unlock();
            }
        }

        //log.info("exit getEntityManagerFactory() ...");
        return emf;
    }

    public static ComputingNode getComputingNode(int nodeId) 
    {
        ComputingNode node;
        CommonServiceConnector csConn = null;

        try 
        {
            csConn = new CommonServiceConnector();
            CommonService.Client commonService = csConn.getClient(commonServiceIPs);

            node = (ComputingNode) Tool.deserializeObject(commonService.getComputingNode(nodeId).array());

            return node;
        } catch (Exception e) {
            log.error("failed to getComputingNode()! e=" + e.getMessage());
            return null;
        } finally {
            csConn.close();
        }
    }

    public static synchronized EntityManagerFactory getPlatformEntityManagerFactory(Map<String, String> properties) {
        try {
            if (systemEmf == null) {
                Map<String, Object> persistenceMap = new HashMap<>();

                persistenceMap.put("hibernate.connection.url", properties.get("url"));
                persistenceMap.put("hibernate.connection.username", properties.get("username"));
                persistenceMap.put("hibernate.connection.password", properties.get("password"));
                // persistenceMap.put("javax.persistence.lock.timeout", CommonKeys.JPA_LOCK_TIMEOUT); // 1 second

                log.info(String.format("Connecting TO db %s: url=%s,username=%s,password=%s", CommonKeys.PLATFORM_DATABASE_NAME, properties.get("url"), properties.get("username"), properties.get("password")));

                systemEmf = Persistence.createEntityManagerFactory(CommonKeys.PLATFORM_DATABASE_NAME, persistenceMap);

                log.info("systemEmf=" + systemEmf);

                return systemEmf;
            } else {
                return systemEmf;
            }
        } catch (Exception e) {
            log.error("failed to getPlatformEntityManagerFactory! e=", e);
            return null;
        }
    }

    public static void resetPlatformEntityManagerFactory() {
        log.error("reset platformEntityManagerFactory!");
        systemEmf = null;
    }

    public static EntityManagerFactory getPlatformEntityManagerFactory() {
        try {
            //log.info(" getPlatformEntityManagerFactory() ...");

            if (systemEmf == null) {
                lockForPlatformEm.lock();// 取得锁

                if (systemEmf == null) {
                    CommonServiceConnector csConn = null;

                    try {
                        csConn = new CommonServiceConnector();
                        CommonService.Client commonService = csConn.getClient(commonServiceIPs);

                        Map<String, String> properties = commonService.getPlatformDBProperties();

                        Map<String, Object> persistenceMap = new HashMap<>();

                        persistenceMap.put("hibernate.connection.url", properties.get("url"));
                        persistenceMap.put("hibernate.connection.username", properties.get("username"));
                        persistenceMap.put("hibernate.connection.password", properties.get("password"));
                        //persistenceMap.put("javax.persistence.lock.timeout", CommonKeys.JPA_LOCK_TIMEOUT); // 1 second

                        log.info(String.format("Connecting TO db %s: url=%s,username=%s,password=%s", CommonKeys.PLATFORM_DATABASE_NAME, properties.get("url"), properties.get("username"), properties.get("password")));

                        systemEmf = Persistence.createEntityManagerFactory(CommonKeys.PLATFORM_DATABASE_NAME, persistenceMap);
                    } catch (Exception e) {
                        log.error("getPlatformEntityManagerFactory() error! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e));
                    } finally {
                        csConn.close();
                        lockForPlatformEm.unlock();
                    }
                } else {
                    lockForPlatformEm.unlock();
                }
            }

            //log.info("exit getPlatformEntityManagerFactory() ...  systemEmf="+systemEmf);
            return systemEmf;
        } catch (Exception e) {
            log.error("failed to getPlatformEntityManagerFactory! e=", e);
            return null;
        }
    }

    public static EntityManagerFactory getOrganizationEntityManagerFactory(Map<String, String> properties) {
        EntityManagerFactory emf = null;

        try {
            Map<String, Object> persistenceMap = new HashMap<>();

            persistenceMap.put("hibernate.connection.url", properties.get("url"));
            persistenceMap.put("hibernate.connection.username", properties.get("username"));
            persistenceMap.put("hibernate.connection.password", properties.get("password"));

            log.info(String.format("Connecting TO db %s: url=%s,username=%s,password=%s", CommonKeys.ORGANIZATION_DATABASE_NAME, properties.get("url"), properties.get("username"), properties.get("password")));

            emf = Persistence.createEntityManagerFactory(CommonKeys.ORGANIZATION_DATABASE_NAME, persistenceMap);
        } catch (Exception e) {
            log.error("getEntityManagerFactory() error! e=" + e);
        }

        return emf;
    }

    public static int[] getAllOrganizationIds() {
        int[] organizationIds = new int[0];
        EntityManager platformEm = null;

        try {
            platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();
            String sql = String.format("select o.id from Organization o where o.parentId > 0");

            List<Integer> organizationList = platformEm.createQuery(sql).getResultList();

            organizationIds = new int[organizationList.size()];

            int i = 0;
            for (Integer id : organizationList) {
                organizationIds[i] = id;
                i++;
            }
        } catch (Exception e) {
            log.error("getAllOrganizationIds() failed! e=" + e.getMessage() + " stacktrace=" + ExceptionUtils.getStackTrace(e));

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }
        } finally {
            if (platformEm != null) {
                if (platformEm.isOpen()) {
                    platformEm.close();
                }
            }
        }

        return organizationIds;
    }

    public static int[] getDataserviceInstanceOrganizations(int dataserviceInstanceId) {
        int[] organizationIds = new int[0];
        EntityManager platformEm = null;

        try {
            platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();

            String sql = String.format("select osi.organizationServiceInstancePK.organizationId from OrganizationServiceInstance osi "
                    + "where osi.serviceInstanceId=%d and osi.organizationServiceInstancePK.serviceInstanceType=%d", dataserviceInstanceId, PlatformServiceType.EDF_DATA_SERVICE.getValue());

            List<Integer> organizationList = platformEm.createQuery(sql).getResultList();

            organizationIds = new int[organizationList.size()];

            int i = 0;
            for (Integer id : organizationList) {
                organizationIds[i] = id;
                i++;
            }
        } catch (Exception e) {
            log.error("getDataserviceInstanceOrganizations() failed! nodeid=" + dataserviceInstanceId + " e=" + e.getMessage());

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }
        } finally {
            if (platformEm != null) {
                if (platformEm.isOpen()) {
                    platformEm.close();
                }
            }
        }

        return organizationIds;
    }

    public static int[] getDataserviceInstanceOrganizations(EntityManager platformEm, int dataserviceInstanceId) {
        int[] organizationIds = new int[0];

        try {
            String sql = String.format("select osi.organizationServiceInstancePK.organizationId from OrganizationServiceInstance osi "
                    + "where osi.serviceInstanceId=%d and osi.organizationServiceInstancePK.serviceInstanceType=%d", dataserviceInstanceId, PlatformServiceType.EDF_DATA_SERVICE.getValue());

            List<Integer> organizationList = platformEm.createQuery(sql).getResultList();

            organizationIds = new int[organizationList.size()];

            int i = 0;
            for (Integer id : organizationList) {
                organizationIds[i] = id;
                i++;
            }
        } catch (Exception e) {
            log.error("getDataserviceInstanceOrganizations() failed! nodeid=" + dataserviceInstanceId + " e=" + e.getMessage());

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }
        }

        return organizationIds;
    }

    public static int getDataserviceInstanceId(int computingNodeId) {
        int dataserviceInstanceId = 0;
        EntityManager platformEm = null;

        try {
            platformEm = Util.getPlatformEntityManagerFactory().createEntityManager();

            String sql = String.format("select sicn.serviceInstanceComputingNodePK.serviceInstanceId from ServiceInstanceComputingNode sicn "
                    + "where sicn.serviceInstanceComputingNodePK.computingNodeId=%d and sicn.serviceInstanceComputingNodePK.serviceInstanceType=%d", computingNodeId, PlatformServiceType.EDF_DATA_SERVICE.getValue());

            dataserviceInstanceId = (Integer) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error("111 getDataserviceInstanceId() failed! nodeid=" + computingNodeId + " e=" + e.getMessage());

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }
        } finally {
            if (platformEm != null) {
                if (platformEm.isOpen()) {
                    platformEm.close();
                }
            }
        }

        return dataserviceInstanceId;
    }

    public static int getDataserviceInstanceId(EntityManager platformEm, int computingNodeId) {
        int dataserviceInstanceId = 0;

        try {
            String sql = String.format("select sicn.serviceInstanceComputingNodePK.serviceInstanceId from ServiceInstanceComputingNode sicn "
                    + "where sicn.serviceInstanceComputingNodePK.computingNodeId=%d and sicn.serviceInstanceComputingNodePK.serviceInstanceType=%d", computingNodeId, PlatformServiceType.EDF_DATA_SERVICE.getValue());

            dataserviceInstanceId = (Integer) platformEm.createQuery(sql).getSingleResult();
        } catch (Exception e) {
            log.error("222 getDataserviceInstanceId() failed! nodeid=" + computingNodeId + " e=" + e.getMessage());

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }
        }

        return dataserviceInstanceId;
    }

    public static List<DatasourceType> getDatasourceTypes() {
        List<DatasourceType> sdd = null;

        try {
            EntityManagerFactory emf = getPlatformEntityManagerFactory();
            sdd = (List<DatasourceType>) emf.createEntityManager().createQuery("from DatasourceType").getResultList();
        } catch (Exception e) {
            log.error("failed to getDatasourceTypes()! e=", e);

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }

            return null;
        }
        return sdd;
    }

    public static List<DataobjectType> getDataobjectTypes() {
        List<DataobjectType> dataobjectTypes = null;
        EntityManager em = null;

        try {
            EntityManagerFactory emf = getPlatformEntityManagerFactory();
            em = emf.createEntityManager();
            dataobjectTypes = (List<DataobjectType>) em.createQuery("from DataobjectType").getResultList();
        } catch (Exception e) {
            log.error("failed to getDataobjectType! e=", e);

            if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                Util.resetPlatformEntityManagerFactory();
            }

            return null;
        } finally {
            if (em != null && em.isOpen()) {
                em.close();
            }
        }

        return dataobjectTypes;
    }

    public static Map<String, String> getServiceInstancePropertyConfigMap(String config) {
        Map<String, String> properties = new HashMap<>();

        Document propertiesXml = Tool.getXmlDocument(config);

        List<Element> list = propertiesXml.selectNodes(CommonKeys.SERVICE_PROPERTIES);

        for (Element element : list) {
            Element nameElement = element.element("name");
            Element valueElement = element.element("value");

            properties.put(nameElement.getTextTrim(), valueElement.getTextTrim());
        }

        return properties;
    }

    public static UnstructuredFileProcessingType getDataProcessingType(int id) {
        EntityManager em = null;
        UnstructuredFileProcessingType dataProcessingType = null;

        try {
            em = Util.getPlatformEntityManagerFactory().createEntityManager();
            dataProcessingType = em.find(UnstructuredFileProcessingType.class, id);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (em != null) {
                em.close();
            }
        }

        return dataProcessingType;
    }

    public static List<String> getDataobjectTypeFieldNames(EntityManager platformEm, int dataobjectTypeId, boolean includeParentType, boolean excludeDataobjectType) throws Exception {
        List<DataobjectType> dataobjectTypes;
        List<String> metadataNames = new ArrayList<>();
        List<Integer> targetDataobjectTypes = new ArrayList<>();

        try {
            dataobjectTypes = platformEm.createQuery("from DataobjectType d order by d.id").getResultList();

            targetDataobjectTypes.add(dataobjectTypeId);

            if (includeParentType) {
                getParentDataobjectTypes(dataobjectTypes, targetDataobjectTypes, dataobjectTypeId, excludeDataobjectType);
            }

            for (Integer objectTypeId : targetDataobjectTypes) {
                for (DataobjectType dataobjectType : dataobjectTypes) {
                    if (dataobjectType.getId().intValue() == objectTypeId.intValue()) {
                        getSingleDataobjectTypeMetadataNames(metadataNames, dataobjectType.getMetadatas(), false);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return metadataNames;
    }

    public static void getSingleDataobjectTypeMetadataNames(List<String> metadtaNames, String metadataXml, boolean withDataType) throws Exception {
        try {
            Document metadataXmlDoc = Tool.getXmlDocument(metadataXml);

            List<Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);

            for (Element node : nodes) {
                String name = node.element(CommonKeys.METADATA_NODE_NAME) != null ? node.element(CommonKeys.METADATA_NODE_NAME).getTextTrim() : "";
                String description = node.element(CommonKeys.METADATA_NODE_DESCRIPTION) != null ? node.element(CommonKeys.METADATA_NODE_DESCRIPTION).getText() : "";

                if (withDataType) {
                    String dataType = node.element(CommonKeys.METADATA_NODE_DATA_TYPE) != null ? node.element(CommonKeys.METADATA_NODE_DATA_TYPE).getText() : "";
                    String inSearchResult = node.element(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT) != null ? node.element(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT).getText() : "";
                    String position = node.element(CommonKeys.METADATA_NODE_SHOW_POSITION) != null ? node.element(CommonKeys.METADATA_NODE_SHOW_POSITION).getText() : "";
                    String selectFrom = node.element(CommonKeys.METADATA_NODE_SELECT_FROM) != null ? node.element(CommonKeys.METADATA_NODE_SELECT_FROM).getText() : "";
                    String stdId = node.element(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID) != null ? node.element(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID).getText() : "0";
                    String indexType = node.element(CommonKeys.METADATA_NODE_INDEX_TYPE) != null ? node.element(CommonKeys.METADATA_NODE_INDEX_TYPE).getText() : "";
                    String metadataGroup = node.element(CommonKeys.METADATA_NODE_METADATA_GROUP) != null ? node.element(CommonKeys.METADATA_NODE_METADATA_GROUP).getText() : "";
                    String flags = node.element(CommonKeys.METADATA_NODE_FLAGS) != null ? node.element(CommonKeys.METADATA_NODE_FLAGS).getTextTrim() : "";

                    String codeId = "0";
                    if (!selectFrom.isEmpty()) {
                        codeId = selectFrom.substring(selectFrom.indexOf(".") + 1);
                    }

                    metadtaNames.add(name + "-" + description + "-" + dataType + "-" + inSearchResult + "-" + position + "-" + codeId + "-" + stdId + "-" + indexType + "-" + metadataGroup + "-" + flags);
                } else {
                    metadtaNames.add(name + "-" + description);
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public static List<String> getDataobjectTypeMetadataNameDescriptions(String metadataXml, String key) throws Exception {
        List<String> names = new ArrayList<>();

        try {
            Document metadataXmlDoc = Tool.getXmlDocument(metadataXml);

            if (key == null) {
                key = "";
            }

            key = key.trim();

            List<Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);

            for (Element node : nodes) {
                String name = node.element(CommonKeys.METADATA_NODE_NAME) != null ? node.element(CommonKeys.METADATA_NODE_NAME).getTextTrim() : "";
                String description = node.element(CommonKeys.METADATA_NODE_DESCRIPTION) != null ? node.element(CommonKeys.METADATA_NODE_DESCRIPTION).getText() : "";

                name = name.trim();
                description = description.trim();

                if (name.toLowerCase().contains(key.toLowerCase())) {
                    names.add(name);
                }

                if (description.toLowerCase().contains(key.toLowerCase())) {
                    names.add(description);
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return names;
    }

    public static int getDataobjectTypeColumnDataType(String metadataName, String metadataXml) throws Exception {
        try {
            if (metadataName == null || metadataName.trim().isEmpty()) {
                return 0;
            }

            Document metadataXmlDoc = Tool.getXmlDocument(metadataXml);

            List<Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);

            for (Element node : nodes) {
                String name = node.element(CommonKeys.METADATA_NODE_NAME) != null ? node.element(CommonKeys.METADATA_NODE_NAME).getTextTrim() : "";

                if (name.trim().equals(metadataName.trim())) {
                    String dataType = node.element(CommonKeys.METADATA_NODE_DATA_TYPE) != null ? node.element(CommonKeys.METADATA_NODE_DATA_TYPE).getText() : "";
                    return Integer.parseInt(dataType);
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return 0;
    }

    public static List<Map<String, Object>> getDataobjectTypeMetadataDefinition(EntityManager platformEm, int dataobjectTypeId, boolean includeParentType, boolean excludeDataobjectType) throws Exception {
        List<DataobjectType> dataobjectTypes;
        List<Map<String, Object>> metadataDefinition = new ArrayList<>();
        List<Integer> targetDataobjectTypes = new ArrayList<>();

        try {
            dataobjectTypes = platformEm.createQuery("from DataobjectType d order by d.id").getResultList();

            if (includeParentType) {
                getParentDataobjectTypes(dataobjectTypes, targetDataobjectTypes, dataobjectTypeId, excludeDataobjectType);
            }

            targetDataobjectTypes.add(dataobjectTypeId);

            for (Integer objectTypeId : targetDataobjectTypes) {
                for (DataobjectType dataobjectType : dataobjectTypes) {
                    if (dataobjectType.getId().intValue() == objectTypeId.intValue()) {
                        getSingleDataobjectTypeMetadataDefinition(metadataDefinition, dataobjectType.getMetadatas());
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }

        return metadataDefinition;
    }

    public static Map<String, Map<String, String>> getColumnValueNameMapping(DataobjectType dataobjectType, List<Map<String, Object>> metadataDefintion, String[] selectedColumns) {
        Map<String, Map<String, String>> columnValueNameMapping = new HashMap<>();

        return columnValueNameMapping;
    }

    public static void getSingleDataobjectTypeMetadataDefinition(List<Map<String, Object>> metadtaDefintion, String metadataXml) throws Exception {
        try {
            Document metadataXmlDoc = Tool.getXmlDocument(metadataXml);
            Map<String, Object> map;

            List<Element> nodes = metadataXmlDoc.selectNodes(CommonKeys.METADATA_NODE);

            for (Element node : nodes) {
                map = new HashMap<>();
                map.put(CommonKeys.METADATA_NODE_NAME, node.element(CommonKeys.METADATA_NODE_NAME) != null ? node.element(CommonKeys.METADATA_NODE_NAME).getTextTrim() : "");
                map.put(CommonKeys.METADATA_NODE_DESCRIPTION, node.element(CommonKeys.METADATA_NODE_DESCRIPTION) != null ? node.element(CommonKeys.METADATA_NODE_DESCRIPTION).getTextTrim() : "");

                String str = node.element(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID) != null ? node.element(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID).getTextTrim() : "0";

                if (str == null || str.trim().isEmpty()) {
                    map.put(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID, 0);
                } else {
                    map.put(CommonKeys.METADATA_NODE_DATA_ITEM_STANDARD_ID, Integer.parseInt(str));
                }

                str = node.element(CommonKeys.METADATA_NODE_METADATA_GROUP) != null ? node.element(CommonKeys.METADATA_NODE_METADATA_GROUP).getTextTrim() : "0";

                if (str == null || str.trim().isEmpty()) {
                    map.put(CommonKeys.METADATA_NODE_METADATA_GROUP, 0);
                } else {
                    map.put(CommonKeys.METADATA_NODE_METADATA_GROUP, Integer.parseInt(str));
                }

                map.put(CommonKeys.METADATA_DATETIME_FORMAT, node.element(CommonKeys.METADATA_DATETIME_FORMAT) != null ? node.element(CommonKeys.METADATA_DATETIME_FORMAT).getTextTrim() : "");

                map.put(CommonKeys.METADATA_NODE_DATA_TYPE, node.element(CommonKeys.METADATA_NODE_DATA_TYPE) != null ? node.element(CommonKeys.METADATA_NODE_DATA_TYPE).getTextTrim() : "");
                map.put(CommonKeys.METADATA_NODE_INDEX_TYPE, node.element(CommonKeys.METADATA_NODE_INDEX_TYPE) != null ? node.element(CommonKeys.METADATA_NODE_INDEX_TYPE).getTextTrim() : String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue())); // default not_analyzed

                map.put(CommonKeys.METADATA_NODE_IS_PRIMARY_KEY, node.element(CommonKeys.METADATA_NODE_IS_PRIMARY_KEY) != null ? Integer.parseInt(node.element(CommonKeys.METADATA_NODE_IS_PRIMARY_KEY).getTextTrim()) : 0); // default 1 - single value
                map.put(CommonKeys.METADATA_NODE_IS_SINGLE_VALUE, node.element(CommonKeys.METADATA_NODE_IS_SINGLE_VALUE) != null ? Integer.parseInt(node.element(CommonKeys.METADATA_NODE_IS_SINGLE_VALUE).getTextTrim()) : 1); // default 1 - single value
                map.put(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT, node.element(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT) != null ? Integer.parseInt(node.element(CommonKeys.METADATA_NODE_IN_SEARCH_RESULT).getTextTrim()) : 0); // default 0 - not in search result

                map.put(CommonKeys.METADATA_NODE_FLAGS, node.element(CommonKeys.METADATA_NODE_FLAGS) != null ? node.element(CommonKeys.METADATA_NODE_FLAGS).getTextTrim() : "");

                map.put(CommonKeys.METADATA_NODE_SEARCH_FACTOR, node.element(CommonKeys.METADATA_NODE_SEARCH_FACTOR) != null ? Float.parseFloat(node.element(CommonKeys.METADATA_NODE_SEARCH_FACTOR).getTextTrim()) : 1.0f); // default 1.0 - not in search result

                map.put(CommonKeys.METADATA_NODE_LENGTH, node.element(CommonKeys.METADATA_NODE_LENGTH) != null ? node.element(CommonKeys.METADATA_NODE_LENGTH).getTextTrim() : "");
                map.put(CommonKeys.METADATA_NODE_PRECISION, node.element(CommonKeys.METADATA_NODE_PRECISION) != null ? node.element(CommonKeys.METADATA_NODE_PRECISION).getTextTrim() : "");
                map.put(CommonKeys.METADATA_NODE_SHOW_POSITION, node.element(CommonKeys.METADATA_NODE_SHOW_POSITION) != null ? node.element(CommonKeys.METADATA_NODE_SHOW_POSITION).getTextTrim() : "");
                map.put(CommonKeys.METADATA_NODE_IS_DEFAULT_TO_SHOW, node.element(CommonKeys.METADATA_NODE_IS_DEFAULT_TO_SHOW) != null ? Integer.parseInt(node.element(CommonKeys.METADATA_NODE_IS_DEFAULT_TO_SHOW).getTextTrim()) : 1);

                String selectFrom = node.element(CommonKeys.METADATA_NODE_SELECT_FROM) != null ? node.element(CommonKeys.METADATA_NODE_SELECT_FROM).getTextTrim() : "";
                if (selectFrom.startsWith("code_system")) {
                    String codeId = selectFrom.substring(selectFrom.indexOf(".") + 1);
                    map.put(CommonKeys.METADATA_NODE_SELECTED_CODE_ID, Integer.parseInt(codeId));
                }

                map.put(CommonKeys.METADATA_NODE_SELECT_FROM, node.element(CommonKeys.METADATA_NODE_SELECT_FROM) != null ? node.element(CommonKeys.METADATA_NODE_SELECT_FROM).getTextTrim() : ""); // select from

                metadtaDefintion.add(map);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private static void getParentDataobjectTypes(List<DataobjectType> dataobjectTypes, List<Integer> targetDataobjectTypes, Integer selectedDataobjectType, boolean excludeDataobjectType) {
        for (DataobjectType type : dataobjectTypes) {
            if (type.getId().intValue() == selectedDataobjectType.intValue()) {
                if (type.getParentType() == 0) {
                    return;
                }

                if (excludeDataobjectType) // exclude dataobject
                {
                    for (DataobjectType parentType : dataobjectTypes) {
                        if (parentType.getId() == type.getParentType()) {
                            if (parentType.getParentType() > 0) {
                                targetDataobjectTypes.add(type.getParentType());
                                break;
                            }
                        }
                    }
                } else {
                    targetDataobjectTypes.add(type.getParentType());
                }

                getParentDataobjectTypes(dataobjectTypes, targetDataobjectTypes, type.getParentType(), excludeDataobjectType);
            }
        }
    }

    // get message from bundle file according to the name
    public static String getBundleMessage(String bundleMessageName) {
        String message = null;

        try {
            message = ResourceBundle.getBundle(CommonKeys.BUNDLENAME, defaultLocale).getString(bundleMessageName);
        } catch (Exception e) {
            message = bundleMessageName;
        }
        return message;
    }

    public static String getErrorMessage(String bundleMessageName, Object... args) {
        String key = bundleMessageName.substring("data_quality_check_error_".length());
        return String.format("%s:%s", key, getBundleMessage(bundleMessageName, args));
    }

    // get compound message from bundle file according to the name, and args
    public static String getBundleMessage(String bundleMessageName, Object... args) {
        return MessageFormat.format(ResourceBundle.getBundle(CommonKeys.BUNDLENAME).getString(bundleMessageName), args);
    }

    public static List<Map<String, String>> prepareSQLDBSyncInfoIndexTypeDefinition() {
        Map<String, String> map;
        List<Map<String, String>> fieldList = new ArrayList<>();

        map = new HashMap<>();
        map.put("name", "organization_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "repository_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "index_type_name");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "status");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "status_time");
        map.put("dataType", MetadataDataType.TIMESTAMP.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "last_update_time");
        map.put("dataType", MetadataDataType.TIMESTAMP.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "items_needed_to_sync");
        map.put("dataType", MetadataDataType.LONG.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        return fieldList;
    }

    public static List<Map<String, String>> prepareResourceLockIndexTypeDefinition() {
        Map<String, String> map;
        List<Map<String, String>> fieldList = new ArrayList<>();

        map = new HashMap<>();
        map.put("name", "organization_id");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "lock_resource_type");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "resourceId");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "status");
        map.put("dataType", MetadataDataType.INTEGER.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "status_time");
        map.put("dataType", MetadataDataType.TIMESTAMP.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "last_update_time");
        map.put("dataType", MetadataDataType.TIMESTAMP.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "locked_client_id");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        map = new HashMap<>();
        map.put("name", "other_info");
        map.put("dataType", MetadataDataType.STRING.getEsType());
        map.put("indexType", String.valueOf(MetadataIndexType.NOT_ANALYZED.getValue()));
        map.put("inSearchResult", "1");
        fieldList.add(map);

        return fieldList;
    }

    public static void closeDBConnection(int organizationId, int repositoryId, String dbConnKey) {
        String key = String.format("%d-%d-%s-dbconn", organizationId, repositoryId, dbConnKey);
        Connection dbConn = (Connection) objectCache.get(key);

        if (dbConn != null) {
            JDBCUtil.close(dbConn);
            objectCache.remove(key);
        }
    }

    public static Map<String, Object> writeDataToSQLDB1(String dbConnKey, List<Map<String, Object>> metadataDefinitions, String tableName, DatasourceConnection datasourceConnection, boolean needEdfIdField, int organizationId, int repositoryId, DataobjectType dataobjectType, List<Map<String, String>> searchResults, boolean isGMTTime, String dateFormat, String datetimeFormat, boolean dataFromEs, boolean commitAfterDone) throws SQLException, Exception {
        String schema;
        String insertSql;
        String updateSql;
        String whereSql;
        String deleteSql;
        Connection conn;
        Statement stmt = null;
        Map<String, Object> resultMap = new HashMap<>();
        int retry;

        DatabaseType databaseType;
        Map<String, String> columnLenPrecisionMap;
        List<String> primaryKeyNames;
        Map<String, String> tableColumnNameMap = new HashMap<>();
        String columnNameStr;
        List<String> tableColumns;
        List<String> metadataColumns;
        String value;
        String insertSqlError;
        String updateSqlError;
        String deleteSqlError;
        String errorInfo;
        String key;
        List<String> addColumnSqlList;
        String tableColumnName;
        boolean found;
        int columnNo;
        boolean isPrimaryKey;
        Date date;
        int k = 0;
        boolean batchProcessing = true;

        try {
            key = String.format("%d-schema", datasourceConnection.getId());
            schema = (String) objectCache.get(key);
            if (schema == null) {
                schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
                objectCache.put(key, schema);
            }

            key = String.format("%d-databaseType", datasourceConnection.getId());
            databaseType = (DatabaseType) objectCache.get(key);
            if (databaseType == null) {
                databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
                objectCache.put(key, databaseType);
            }

            key = String.format("%d-%d-%s-dbconn", organizationId, repositoryId, dbConnKey);
            conn = (Connection) objectCache.get(key);

            if (conn != null) {
                if (conn.isClosed()) {
                    conn = null;
                }
            }

            if (conn == null) {
                log.info("geting new db connection ! key=" + key);

                conn = JDBCUtil.getJdbcConnection(datasourceConnection);
                conn.setAutoCommit(false);

                objectCache.put(key, conn);
            }

            log.info(" db connection=" + conn + ",key=" + key + " isclose=" + conn.isClosed());

            key = String.format("%d-primaryKeyNames", dataobjectType.getId());
            primaryKeyNames = (List<String>) objectCache.get(key);
            if (primaryKeyNames == null) {
                primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());
                objectCache.put(key, primaryKeyNames);
            }

            key = String.format("%d-columnLenPrecisionMap", dataobjectType.getId());
            columnLenPrecisionMap = (Map<String, String>) objectCache.get(key);
            if (columnLenPrecisionMap == null) {
                columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(dataobjectType.getMetadatas());
                objectCache.put(key, columnLenPrecisionMap);
            }

            Map<String, String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            String tableSpace = properties.get("table_space");
            String indexSpace = properties.get("index_space");

            tableColumns = new ArrayList<>();
            metadataColumns = new ArrayList<>();
            columnNameStr = "";

            for (Map<String, Object> definition : metadataDefinitions) {
                String name = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                tableColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(), name, true);

                tableColumns.add(tableColumnName);
                metadataColumns.add(name);
                columnNameStr += tableColumnName + ",";
                tableColumnNameMap.put(tableColumnName, name);
            }

            if (needEdfIdField) {
                tableColumnName = "edf_id";
                tableColumns.add(tableColumnName);
                columnNameStr += tableColumnName + ",";
                tableColumnNameMap.put(tableColumnName, tableColumnName);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length() - 1);

            while (true) {
                k = 0;

                log.info("write to db .... size=" + searchResults.size() + "  needEdfIdField=" + needEdfIdField);
                stmt = conn.createStatement();
                //stmt.setQueryTimeout(CommonKeys.JDBC_QUERY_TIMEOUT);

                for (Map<String, String> result : searchResults) {
                    //Map<String,String> resultFields = (Map<String,String>)result.get("fields");

                    JSONObject resultFields = new JSONObject(result.get("fields"));

                    updateSql = String.format("UPDATE %s SET ", tableName);
                    whereSql = String.format("WHERE ");
                    deleteSql = String.format("DELETE from %s ", tableName);

                    String edfId = (String) result.get("id");
                    edfId = edfId.substring(0, 40);

                    insertSql = String.format("INSERT INTO %s ( %s ) VALUES (", tableName, columnNameStr);

                    for (String tableColumnStr : tableColumns) {
                        String metadataName = tableColumnNameMap.get(tableColumnStr);

                        if (dataFromEs) {
                            value = String.valueOf(resultFields.optString(metadataName));
                        } else {
                            value = String.valueOf(resultFields.optString(tableColumnStr));
                        }

                        if (needEdfIdField && tableColumnStr.equals("edf_id")) {
                            value = edfId;
                        }

                        if (value == null || value.equals("null")) {
                            value = "";
                        } else {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);

                            if (!value.isEmpty() && Tool.isNumber(value)) {
                                value = Tool.normalizeNumber(value);
                            }
                        }

                        value = Tool.removeQuotation(value);

                        int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                        if (dataType == 0) {
                            dataType = 1;
                        }

                        if (dataType == MetadataDataType.DATE.getValue()) {
                            date = Tool.convertStringToDate(value, dateFormat);

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToDateString1(date);
                        } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                            date = Tool.convertStringToDate(value, datetimeFormat);

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                        }

                        if (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) {
                            if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                                    || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                                    || dataType == MetadataDataType.DOUBLE.getValue()) {
                                if (value.isEmpty()) {
                                    String clp = columnLenPrecisionMap.get(metadataName);
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    insertSql += String.format("'%s',", defaultNullVal);
                                } else {
                                    insertSql += String.format("%s,", value);
                                }
                            } else if (dataType == MetadataDataType.DATE.getValue()) {
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),", value);
                            } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                                insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),", value.substring(0, 19));
                            } else {
                                insertSql += String.format("'%s',", value);
                            }
                        } else if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                                || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                                || dataType == MetadataDataType.DOUBLE.getValue()) {
                            if (value.isEmpty()) {
                                String clp = columnLenPrecisionMap.get(metadataName);
                                String defaultNullVal = Tool.getDefaultNullVal(clp);

                                insertSql += String.format("'%s',", defaultNullVal);
                            } else {
                                insertSql += String.format("%s,", value);
                            }
                        } else {
                            insertSql += String.format("'%s',", value);
                        }
                    }

                    insertSql = insertSql.substring(0, insertSql.length() - 1) + ")";

                    columnNo = 0;

                    for (String rKey : resultFields.keySet()) {
                        String metadataName = rKey;
                        value = resultFields.optString(rKey);

                        if (dataFromEs) {
                            if (!metadataName.startsWith(dataobjectType.getName().trim().toUpperCase()) && !metadataName.startsWith(dataobjectType.getName().trim().toLowerCase())) {
                                continue;
                            }
                        } else {
                            metadataName = dataobjectType.getName().trim() + "_" + metadataName;
                        }

                        columnNo++;

                        String tableColumnStr = Tool.changeTableColumnName(dataobjectType.getName().trim(), metadataName, true);

                        if (needEdfIdField && tableColumnStr.equals("edf_id")) {
                            value = edfId;
                        }

                        if (value == null || value.equals("null")) {
                            value = "";
                        } else {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);

                            if (!value.isEmpty() && Tool.isNumber(value)) {
                                value = Tool.normalizeNumber(value);
                            }
                        }

                        value = Tool.removeQuotation(value);

                        int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                        if (dataType == 0) {
                            dataType = 1;
                        }

                        if (dataType == MetadataDataType.DATE.getValue()) {
                            date = Tool.convertStringToDate(value, dateFormat);

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToDateString(date);
                        } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                            date = Tool.convertStringToDate(value, datetimeFormat);

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                        }

                        // for updatesql
                        isPrimaryKey = false;

                        for (String kName : primaryKeyNames) {
                            if (kName.toLowerCase().equals(metadataName.toLowerCase())) {
                                isPrimaryKey = true;
                                break;
                            }
                        }

                        if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                                || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                                || dataType == MetadataDataType.DOUBLE.getValue()) {
                            if (value.isEmpty()) {
                                String clp = columnLenPrecisionMap.get(metadataName);
                                String defaultNullVal = Tool.getDefaultNullVal(clp);

                                if (isPrimaryKey) {
                                    whereSql += String.format(" %s = '%s' AND", tableColumnStr, defaultNullVal);
                                } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                                    updateSql += String.format(" %s = '%s',", tableColumnStr, defaultNullVal);
                                }
                            } else if (isPrimaryKey) {
                                whereSql += String.format(" %s = %s AND", tableColumnStr, value);
                            } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                                updateSql += String.format(" %s = %s,", tableColumnStr, value);
                            }
                        } else if (isPrimaryKey) {
                            whereSql += String.format(" %s = '%s' AND", tableColumnStr, value);
                        } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                            updateSql += String.format(" %s = '%s',", tableColumnStr, value);
                        }
                    }

                    if (needEdfIdField && primaryKeyNames.isEmpty()) // has no key
                    {
                        whereSql += String.format(" edf_id='%s' AND", edfId);
                    }

                    updateSql = updateSql.substring(0, updateSql.length() - 1);

                    whereSql = whereSql.substring(0, whereSql.length() - 3);

                    updateSql += " " + whereSql;

                    deleteSql += whereSql;

                    retry = 0;
                    int ret = 0;

                    while (true) {
                        //log.info(" start to execute sql...");

                        retry++;

                        insertSqlError = "";
                        updateSqlError = "";
                        deleteSqlError = "";

                        if (primaryKeyNames.isEmpty()) // no primary key
                        {
                            try {
                                ret = stmt.executeUpdate(deleteSql);
                                log.debug(" delete ret=" + ret + " deleteSql=" + deleteSql);
                                //log.info(" delete ret="+ret);
                            } catch (Exception e) {
                                deleteSqlError = "deleteSql failed! e=" + e + " delete sql=" + deleteSql;
                                log.error(deleteSqlError);

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                }
                            }
                        }

                        try {
                            if (batchProcessing == true) {
                                stmt.addBatch(insertSql);
                            } else {
                                ret = stmt.executeUpdate(insertSql);
                            }
                            //log.debug("successfully insert ret="+ret+" insertSql="+insertSql);
                            //log.info("successfully insert ret="+ret);

                            k++;
                            break;
                        } catch (Exception e) {
                            insertSqlError = "insert database failed! e=" + e + " insert sql=" + insertSql;
                            //log.error(insertSqlError);

                            try {
                                conn.commit();
                            } catch (Exception ee) {
                            }

                            try {
                                ret = stmt.executeUpdate(updateSql);
                                //log.debug(" update ret="+ret+" updateSql="+updateSql);
                                //log.info(" update ret="+ret);

                                if (ret == 1) {
                                    k++;
                                    break;
                                }
                            } catch (Exception ee) {
                                updateSqlError = "updateSql failed! e=" + ee + " update sql=" + updateSql;
                                //log.error(updateSqlError);

                                try {
                                    conn.commit();
                                } catch (Exception eee) {
                                }
                            }
                        }

                        errorInfo = insertSqlError + " " + updateSqlError + " " + deleteSqlError;

                        log.error("retry=" + retry + ", errorInfo=" + errorInfo);

                        if (errorInfo.toLowerCase().contains("deadlock")) {
                            retry = 3;
                        } else if (errorInfo.toLowerCase().contains("column")) {
                            try {
                                if (JDBCUtil.isTableExists(conn, schema, tableName)) {
                                    log.info("table name =" + tableName + " exists" + " needEdfIdField=" + needEdfIdField);

                                    List<String> existingColumnNames = JDBCUtil.getTableColumnNames(conn, schema, tableName.toLowerCase());
                                    log.info(" existing column size=" + existingColumnNames.size());

                                    if (needEdfIdField) // check edf_id
                                    {
                                        found = false;

                                        for (String existingColumnName : existingColumnNames) {
                                            log.info("111 existing column name=" + existingColumnName);

                                            if (existingColumnName.equals("edf_id")) {
                                                found = true;
                                                break;
                                            }
                                        }

                                        if (!found) {
                                            addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType, tableName, "edf_id", "VARCHAR(40)", "");

                                            try {
                                                for (String addColumnSql : addColumnSqlList) {
                                                    log.info(" exceute add edf_id column sql =" + addColumnSql);
                                                    stmt.executeUpdate(addColumnSql);
                                                }

                                                String sqlStr = String.format("CREATE INDEX %s_edf_id_index ON %s (edf_id)", tableName, tableName);
                                                log.info(" exceute sql =" + sqlStr);
                                                stmt.executeUpdate(sqlStr);

                                                conn.commit();
                                            } catch (Exception e) {
                                                log.error(" add column failed! e=" + e);

                                                try {
                                                    conn.commit();
                                                } catch (Exception ee) {
                                                }
                                            }
                                        }
                                    }

                                    for (Map<String, Object> definition : metadataDefinitions) {
                                        String metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                                        String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                        String dataType = (String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                        String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                                        int len;

                                        if (lenStr == null || lenStr.trim().isEmpty()) {
                                            len = 0;
                                        } else {
                                            len = Integer.parseInt(lenStr);
                                        }

                                        log.info(" dataobject name=" + dataobjectType.getName() + " metadataName=" + metadataName);
                                        String newColumnName = metadataName.toLowerCase().replace(dataobjectType.getName().trim().toLowerCase() + "_", "");
                                        newColumnName = newColumnName.toUpperCase();

                                        log.info(" newColumnName=" + newColumnName);

                                        found = false;

                                        for (String existingColumnName : existingColumnNames) {
                                            if (newColumnName.equals(existingColumnName.toUpperCase())) {
                                                found = true;
                                                break;
                                            }
                                        }

                                        if (!found) {
                                            String sqlType = MetadataDataType.findByValue(Integer.parseInt(dataType)).getJdbcType();

                                            if (sqlType.contains("%d")) {
                                                sqlType = String.format(sqlType, len);
                                            }

                                            addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType, tableName, newColumnName, sqlType, description);

                                            try {
                                                for (String addColumnSql : addColumnSqlList) {
                                                    log.info("111 exceute add column sql =" + addColumnSql);
                                                    stmt.executeUpdate(addColumnSql);
                                                }
                                            } catch (Exception e) {
                                                log.error(" add column failed! e=" + e);
                                            }

                                            try {
                                                conn.commit();
                                            } catch (Exception ee) {
                                            }
                                        }
                                    }
                                } else {
                                    log.info("table name =" + tableName + " not exists");

                                    List<String> sqlList = Util.createTableForDataobjectType(needEdfIdField, true, dataobjectType.getName(), dataobjectType.getDescription(), tableName, "", primaryKeyNames, databaseType, datasourceConnection.getUserName(), metadataDefinitions, metadataColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                                    try {
                                        for (String createTableSql : sqlList) {
                                            log.info(" exceute create table sql =" + createTableSql);
                                            stmt.executeUpdate(createTableSql);
                                        }

                                        conn.commit();
                                    } catch (Exception e) {
                                        log.error("2222 add column failed! e=" + e);

                                        try {
                                            conn.commit();
                                        } catch (Exception ee) {
                                        }
                                    }

                                    throw new Exception("table name =" + tableName + " not exists");
                                }
                            } catch (Exception e) {
                                errorInfo += " update/create table failed! try again ! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e);
                                log.error(errorInfo);

                                Tool.SleepAWhile(1, 0);

                                if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                                    Util.resetPlatformEntityManagerFactory();
                                    Util.resetEntityManagerFactory(organizationId);
                                }

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                }
                            }
                        }

                        if (retry >= 2 || stmt == null) {
                            try {
                                conn.rollback();
                            } catch (Exception e) {
                            }

                            //key = String.format("%d-%d-dbconn",organizationId,repositoryId);
                            //objectCache.put(key, null);
                            JDBCUtil.close(stmt);
                            //JDBCUtil.close(conn);

                            conn = null;
                            stmt = null;

                            log.error(" after retry " + retry + ", throw exception!");

                            throw new Exception(errorInfo);
                        }
                    }

                    if (k % 1000 == 0) {
                        log.info("2222222222 processed [" + k + "] items!");
                    }
                }

                if (batchProcessing == true) {
                    try {
                        Date startTime = new Date();

                        stmt.executeBatch();

                        log.info(String.format("finish execute batch!!! took %d ms, size=%d", (new Date().getTime() - startTime.getTime()), k));
                    } catch (Exception e) {
                        errorInfo = "7777777777777777 execute batch failed! e=" + e;
                        log.error(errorInfo);

                        try {
                            conn.commit();
                        } catch (Throwable ee) {
                        }

                        batchProcessing = false;
                        JDBCUtil.close(stmt);
                        continue;
                    }
                }

                break;
            }

            if (commitAfterDone) {
                JDBCUtil.close(stmt);
                conn.commit();
                log.info(" 6666666666666 commit()! size=" + searchResults.size());
            }

            resultMap.put("conn", conn);

            return resultMap;
        } catch (Throwable e) {
            log.error(" processing failed!  e=" + e + " stacktrace = " + ExceptionUtils.getStackTrace(e));

            key = String.format("%d-%d-%s-dbconn", organizationId, repositoryId, dbConnKey);
            log.error("66666666666666 reset dbConn!!!");

            conn = (Connection) objectCache.get(key);
            JDBCUtil.close(conn);

            objectCache.put(key, null);

            throw e;
        } finally {
            //JDBCUtil.close(conn);            
        }
    }

    public static Map<String, Object> writeDataToSQLDBWithPreparedStatmentFromES(String dbConnKey, List<Map<String, Object>> metadataDefinitions, String tableName, DatasourceConnection datasourceConnection, boolean needEdfIdField, int organizationId, int repositoryId, DataobjectType dataobjectType, List<Map<String, String>> searchResults, boolean isGMTTime, String dateFormat, String datetimeFormat) throws SQLException, Exception {
        String schema;
        String insertSql;
        String updateSql;
        String whereSql;
        String deleteSql;
        PreparedStatement insertStmt;
        PreparedStatement updateStmt;
        PreparedStatement deleteStmt;
        Connection conn;
        Statement stmt = null;
        Map<String, Object> resultMap = new HashMap<>();
        int retry;

        DatabaseType databaseType;
        Map<String, String> columnLenPrecisionMap;
        List<String> primaryKeyNames;
        Map<String, String> tableColumnNameMap = new HashMap<>();
        String questionMarkStr;
        String columnNameStr;
        List<String> tableColumns;
        List<String> metadataColumns;
        String value;
        String insertSqlError;
        String updateSqlError;
        String deleteSqlError;
        String errorInfo;
        String key;
        List<String> addColumnSqlList;
        String tableColumnName;
        boolean found;
        int columnNo;
        boolean isPrimaryKey;
        Date date;
        int k = 0;
        boolean batchProcessing = true;

        try {
            key = String.format("%d-schema", datasourceConnection.getId());
            schema = (String) objectCache.get(key);
            if (schema == null) {
                schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
                objectCache.put(key, schema);
            }

            key = String.format("%d-databaseType", datasourceConnection.getId());
            databaseType = (DatabaseType) objectCache.get(key);
            if (databaseType == null) {
                databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
                objectCache.put(key, databaseType);
            }

            key = String.format("%d-%d-%s-dbconn", organizationId, repositoryId, dbConnKey);
            conn = (Connection) objectCache.get(key);

            if (conn != null) {
                if (conn.isClosed()) {
                    conn = null;
                }
            }

            if (conn == null) {
                log.info("geting new db connection ! key=" + key);

                conn = JDBCUtil.getJdbcConnection(datasourceConnection);
                conn.setAutoCommit(false);

                objectCache.put(key, conn);
            }

            log.info(" db connection=" + conn + ",key=" + key + " isclose=" + conn.isClosed());

            key = String.format("%d-primaryKeyNames", dataobjectType.getId());
            primaryKeyNames = (List<String>) objectCache.get(key);
            if (primaryKeyNames == null) {
                primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());
                objectCache.put(key, primaryKeyNames);
            }

            log.info(" primarKeyname size=" + primaryKeyNames.size());

            key = String.format("%d-columnLenPrecisionMap", dataobjectType.getId());
            columnLenPrecisionMap = (Map<String, String>) objectCache.get(key);
            if (columnLenPrecisionMap == null) {
                columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(dataobjectType.getMetadatas());
                objectCache.put(key, columnLenPrecisionMap);
            }

            Map<String, String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            String tableSpace = properties.get("table_space");
            String indexSpace = properties.get("index_space");

            tableColumns = new ArrayList<>();
            metadataColumns = new ArrayList<>();
            columnNameStr = "";
            questionMarkStr = "";

            for (Map<String, Object> definition : metadataDefinitions) {
                String name = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                tableColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(), name, true);

                if (databaseType == DatabaseType.GREENPLUM) {
                    if (JDBCUtil.isGPReservedWord(tableColumnName)) {
                        tableColumnName += "_edf";
                    }
                }

                tableColumns.add(tableColumnName);
                metadataColumns.add(name);
                columnNameStr += tableColumnName + ",";
                questionMarkStr += "?,";
                tableColumnNameMap.put(tableColumnName, name);
            }

            if (needEdfIdField) {
                tableColumnName = "edf_id";
                tableColumns.add(tableColumnName);
                columnNameStr += tableColumnName + ",";
                questionMarkStr += "?,";
                tableColumnNameMap.put(tableColumnName, tableColumnName);

                tableColumnName = "edf_repository_id";
                tableColumns.add(tableColumnName);
                columnNameStr += tableColumnName + ",";
                questionMarkStr += "?,";
                tableColumnNameMap.put(tableColumnName, tableColumnName);

                tableColumnName = "edf_last_modified_time";
                tableColumns.add(tableColumnName);
                columnNameStr += tableColumnName + ",";
                questionMarkStr += "?,";
                tableColumnNameMap.put(tableColumnName, tableColumnName);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length() - 1);
            questionMarkStr = questionMarkStr.substring(0, questionMarkStr.length() - 1);

            insertSql = String.format("INSERT INTO %s ( %s ) VALUES (%s)", tableName, columnNameStr, questionMarkStr);

            while (true) {
                k = 0;

                insertStmt = conn.prepareStatement(insertSql);

                if (batchProcessing == false || primaryKeyNames.isEmpty()) {
                    stmt = conn.createStatement();
                }

                log.info("write to db .... size=" + searchResults.size() + "  needEdfIdField=" + needEdfIdField);
                //stmt = conn.createStatement(); 
                //stmt.setQueryTimeout(CommonKeys.JDBC_QUERY_TIMEOUT);

                for (Map<String, String> result : searchResults) {
                    //Map<String,String> resultFields = (Map<String,String>)result.get("fields");
                    JSONObject resultFields = new JSONObject(result.get("fields"));

                    updateSql = String.format("UPDATE %s SET ", tableName);
                    whereSql = String.format("WHERE ");
                    deleteSql = String.format("DELETE from %s ", tableName);

                    String edfId = (String) result.get("id");
                    edfId = edfId.substring(0, 40);

                    int jj = 0;
                    int sqlType;
                    for (String tableColumnStr : tableColumns) {
                        jj++;

                        if (needEdfIdField && (tableColumnStr.equals("edf_id") || tableColumnStr.equals("edf_repository_id")
                                || tableColumnStr.equals("edf_last_modified_time"))) {
                            if (needEdfIdField && tableColumnStr.equals("edf_id")) {
                                sqlType = MetadataDataType.STRING.getSqlType();
                                insertStmt.setObject(jj, edfId, sqlType);
                            } else if (needEdfIdField && tableColumnStr.equals("edf_repository_id")) {
                                sqlType = MetadataDataType.INTEGER.getSqlType();
                                insertStmt.setObject(jj, repositoryId, sqlType);
                            } else if (needEdfIdField && tableColumnStr.equals("edf_last_modified_time")) {
                                sqlType = MetadataDataType.TIMESTAMP.getSqlType();
                                insertStmt.setObject(jj, new Date(), sqlType);
                            }

                            continue;
                        }

                        String metadataName = tableColumnNameMap.get(tableColumnStr);

                        value = String.valueOf(resultFields.optString(metadataName));

                        if (value == null || value.equals("null")) {
                            value = "";
                        } else {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);

                            if (!value.isEmpty() && Tool.isNumber(value)) {
                                value = Tool.normalizeNumber(value);
                            }
                        }

                        value = Tool.removeQuotation(value);

                        int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                        if (dataType == 0) {
                            dataType = 1;
                        }

                        if (dataType == MetadataDataType.DATE.getValue()) {
                            if (!dateFormat.isEmpty()) {
                                date = Tool.convertStringToDate(value, dateFormat);
                            } else {
                                date = new Date(Long.parseLong(value));
                            }

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToDateString1(date);
                        } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                            if (!datetimeFormat.isEmpty()) {
                                date = Tool.convertStringToDate(value, datetimeFormat);
                            } else {
                                date = new Date(Long.parseLong(value));
                            }

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                        }

                        sqlType = MetadataDataType.findByValue(dataType).getSqlType();

                        Object obj = getValueObject(value, dataType);

                        insertStmt.setObject(jj, obj, sqlType);
                    }

                    //insertSql = insertSql.substring(0,insertSql.length()-1)+")";
                    //log.info(" primaryKeyName.size="+primaryKeyNames.size());
                    if (primaryKeyNames.isEmpty() || batchProcessing == false) {
                        columnNo = 0;

                        for (String rKey : resultFields.keySet()) {
                            String metadataName = rKey;
                            value = resultFields.optString(rKey);

                            //log.info(" 33333 metadataName="+metadataName+" value="+value);
                            if (!metadataName.startsWith(dataobjectType.getName().trim().toUpperCase()) && !metadataName.startsWith(dataobjectType.getName().trim().toLowerCase())) {
                                //log.warn(" metadataName not start with dataobject type name! metadataName="+metadataName+" dataobjectTypeName="+dataobjectType.getName());
                                continue;
                            }

                            columnNo++;

                            String tableColumnStr = Tool.changeTableColumnName(dataobjectType.getName().trim(), metadataName, true);

                            if (databaseType == DatabaseType.GREENPLUM) {
                                if (JDBCUtil.isGPReservedWord(tableColumnStr)) {
                                    tableColumnStr += "_edf";
                                }
                            }

                            if (needEdfIdField && tableColumnStr.equals("edf_id")) {
                                value = edfId;
                            }

                            if (value == null || value.equals("null")) {
                                value = "";
                            } else {
                                value = value.trim();

                                value = Tool.removeSpecialChar(value);

                                if (!value.isEmpty() && Tool.isNumber(value)) {
                                    value = Tool.normalizeNumber(value);
                                }
                            }

                            value = Tool.removeQuotation(value);

                            int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                            //log.info(" 33333 metadataName="+metadataName+" value="+value+" datatype="+dataType);
                            if (dataType == 0) {
                                dataType = 1;
                            }

                            if (dataType == MetadataDataType.DATE.getValue()) {
                                date = Tool.convertStringToDate(value, dateFormat);

                                if (isGMTTime) {
                                    date = Tool.changeFromGmt(date);
                                }

                                value = Tool.convertDateToDateString(date);
                            } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                                date = Tool.convertStringToDate(value, datetimeFormat);

                                if (isGMTTime) {
                                    date = Tool.changeFromGmt(date);
                                }

                                value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                            }

                            // for updatesql
                            isPrimaryKey = false;

                            for (String kName : primaryKeyNames) {
                                if (kName.toLowerCase().equals(metadataName.toLowerCase())) {
                                    isPrimaryKey = true;
                                    break;
                                }
                            }

                            if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                                    || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                                    || dataType == MetadataDataType.DOUBLE.getValue()) {
                                if (value.isEmpty()) {
                                    String clp = columnLenPrecisionMap.get(metadataName);
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    if (isPrimaryKey) {
                                        whereSql += String.format(" %s = '%s' AND", tableColumnStr, defaultNullVal);
                                    } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                                        updateSql += String.format(" %s = '%s',", tableColumnStr, defaultNullVal);
                                    }
                                } else if (isPrimaryKey) {
                                    whereSql += String.format(" %s = %s AND", tableColumnStr, value);
                                } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                                    updateSql += String.format(" %s = %s,", tableColumnStr, value);
                                }
                            } else if (isPrimaryKey) {
                                whereSql += String.format(" %s = '%s' AND", tableColumnStr, value);
                            } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                                updateSql += String.format(" %s = '%s',", tableColumnStr, value);
                            }
                        }

                        if (columnNo == 0) {
                            log.warn("111111111111111111 columnName == 0!!! resultFields=" + resultFields);
                        }

                        /* if ( needEdfIdField && primaryKeyNames.isEmpty() ) // has no key
                        {
                            whereSql += String.format(" edf_id='%s' AND",edfId);
                        }*/
                        if (needEdfIdField) // has no key
                        {
                            value = Tool.convertDateToTimestampStringWithMilliSecond1(new Date());
                            updateSql += String.format(" edf_last_modified_time = '%s',", value);
                        }

                        updateSql = updateSql.substring(0, updateSql.length() - 1);

                        whereSql = whereSql.substring(0, whereSql.length() - 3);

                        updateSql += " " + whereSql;

                        deleteSql += whereSql;
                    }

                    retry = 0;
                    int ret = 0;

                    while (true) {
                        //log.info(" start to execute sql...");

                        retry++;

                        insertSqlError = "";
                        updateSqlError = "";
                        deleteSqlError = "";

                        if (primaryKeyNames.isEmpty()) // no primary key
                        {
                            try {
                                ret = stmt.executeUpdate(deleteSql);
                                //log.debug(" delete ret="+ret+" deleteSql="+deleteSql);
                                log.info(" delete ret=" + ret);
                            } catch (Exception e) {
                                deleteSqlError = "deleteSql failed! e=" + e + " delete sql=" + deleteSql;
                                log.error(deleteSqlError);

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                }
                            }
                        }

                        try {
                            if (batchProcessing == true) {
                                insertStmt.addBatch();
                            } else {
                                ret = insertStmt.executeUpdate();
                                //log.debug("successfully insert ret="+ret+" insertSql="+insertSql);
                                log.info("k=" + k + " successfully insert single record! ret=" + ret);

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                    log.error("commit failed! e=" + ee);
                                }
                            }

                            k++;
                            break;
                        } catch (Exception e) {
                            //insertSqlError  = "insert database failed! e="+e+" insert sql="+insertSql;
                            insertSqlError = "insert database failed! e=" + e;
                            //log.error(insertSqlError);

                            try {
                                conn.commit();
                            } catch (Exception ee) {
                            }

                            try {
                                ret = stmt.executeUpdate(updateSql);
                                //log.debug(" update ret="+ret+" updateSql="+updateSql);
                                log.info("k=" + k + " update single record. ret=" + ret);
                                if (ret == 1) {
                                    try {
                                        conn.commit();
                                    } catch (Exception ee) {
                                        log.error("commit failed! e=" + ee);
                                    }

                                    k++;
                                    break;
                                }

                                if (ret < 1) {
                                    log.info("updateSql failed! ret<1 !!! ret=" + ret + " updateSql=" + updateSql);
                                    throw new Exception(" update failed!");
                                }
                            } catch (Exception ee) {
                                try {
                                    conn.commit();
                                } catch (Exception eee) {
                                }

                                if (updateSql.contains("SET WHERE")) // all column are keys
                                {
                                    break;
                                }

                                updateSqlError = "222222 updateSql failed! e=" + ee + " update sql=" + updateSql;
                                //updateSqlError  = "updateSql failed! e="+ee;
                                log.error(updateSqlError);
                            }
                        }

                        errorInfo = insertSqlError + " " + updateSqlError + " " + deleteSqlError;

                        log.error("1111retry=" + retry + ", errorInfo=" + errorInfo);

                        if (errorInfo.toLowerCase().contains("deadlock")) {
                            retry = 3;
                        } else if (errorInfo.toLowerCase().contains("column")) {
                            try {
                                if (JDBCUtil.isTableExists(conn, schema, tableName)) {
                                    log.info("table name =" + tableName + " exists" + " needEdfIdField=" + needEdfIdField);

                                    List<String> existingColumnNames = JDBCUtil.getTableColumnNames(conn, schema, tableName.toLowerCase());
                                    log.info(" existing column size=" + existingColumnNames.size());

                                    if (needEdfIdField) // check edf_id
                                    {
                                        found = false;

                                        for (String existingColumnName : existingColumnNames) {
                                            log.info("111 existing column name=" + existingColumnName);

                                            if (existingColumnName.equals("edf_id")) {
                                                found = true;
                                                break;
                                            }
                                        }

                                        if (!found) {
                                            addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType, tableName, "edf_id", "VARCHAR(40)", "");

                                            try {
                                                for (String addColumnSql : addColumnSqlList) {
                                                    log.info(" exceute add edf_id column sql =" + addColumnSql);
                                                    stmt.executeUpdate(addColumnSql);
                                                }

                                                String sqlStr = String.format("CREATE INDEX %s_edf_id_index ON %s (edf_id)", tableName, tableName);
                                                log.info(" exceute sql =" + sqlStr);
                                                stmt.executeUpdate(sqlStr);

                                                conn.commit();
                                            } catch (Exception e) {
                                                log.error(" add column failed! e=" + e);

                                                try {
                                                    conn.commit();
                                                } catch (Exception ee) {
                                                }
                                            }
                                        }
                                    }

                                    for (Map<String, Object> definition : metadataDefinitions) {
                                        String metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                                        String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                        String dataType = (String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                        String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                                        int len;

                                        if (lenStr == null || lenStr.trim().isEmpty()) {
                                            len = 0;
                                        } else {
                                            len = Integer.parseInt(lenStr);
                                        }

                                        log.info(" dataobject name=" + dataobjectType.getName() + " metadataName=" + metadataName);
                                        String newColumnName = metadataName.toLowerCase().replace(dataobjectType.getName().trim().toLowerCase() + "_", "");
                                        newColumnName = newColumnName.toUpperCase();

                                        log.info(" newColumnName=" + newColumnName);

                                        found = false;

                                        for (String existingColumnName : existingColumnNames) {
                                            if (newColumnName.equals(existingColumnName.toUpperCase())) {
                                                found = true;
                                                break;
                                            }
                                        }

                                        if (!found) {
                                            String sqlTypeStr = MetadataDataType.findByValue(Integer.parseInt(dataType)).getJdbcType();

                                            if (sqlTypeStr.contains("%d")) {
                                                sqlTypeStr = String.format(sqlTypeStr, len);
                                            }

                                            addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType, tableName, newColumnName, sqlTypeStr, description);

                                            try {
                                                for (String addColumnSql : addColumnSqlList) {
                                                    log.info("111 exceute add column sql =" + addColumnSql);
                                                    stmt.executeUpdate(addColumnSql);
                                                }
                                            } catch (Exception e) {
                                                log.error(" add column failed! e=" + e);
                                            }

                                            try {
                                                conn.commit();
                                            } catch (Exception ee) {
                                            }
                                        }
                                    }
                                } else {
                                    log.info("table name =" + tableName + " not exists");

                                    List<String> sqlList = Util.createTableForDataobjectType(needEdfIdField, true, dataobjectType.getName(), dataobjectType.getDescription(), tableName, "", primaryKeyNames, databaseType, datasourceConnection.getUserName(), metadataDefinitions, metadataColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                                    try {
                                        for (String createTableSql : sqlList) {
                                            log.info(" exceute create table sql =" + createTableSql);
                                            stmt.executeUpdate(createTableSql);
                                        }

                                        conn.commit();
                                    } catch (Exception e) {
                                        log.error("2222 add column failed! e=" + e);

                                        try {
                                            conn.commit();
                                        } catch (Exception ee) {
                                        }
                                    }

                                    throw new Exception("table name =" + tableName + " not exists");
                                }
                            } catch (Exception e) {
                                errorInfo += " update/create table failed! try again ! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e);
                                log.error(errorInfo);

                                Tool.SleepAWhile(1, 0);

                                if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                                    Util.resetPlatformEntityManagerFactory();
                                    Util.resetEntityManagerFactory(organizationId);
                                }

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                }
                            }
                        }

                        if (retry >= 2 || stmt == null) {
                            try {
                                conn.rollback();
                            } catch (Exception e) {
                            }

                            //key = String.format("%d-%d-dbconn",organizationId,repositoryId);
                            //objectCache.put(key, null);
                            JDBCUtil.close(stmt);
                            JDBCUtil.close(insertStmt);

                            conn = null;
                            stmt = null;

                            log.error(" after retry " + retry + ", throw exception!");

                            throw new Exception(errorInfo);
                        }
                    }

                    if (k % 1000 == 0) {
                        log.info("2222222222 processed [" + k + "] items!");
                    }
                }

                if (batchProcessing == true) {
                    try {
                        Date startTime = new Date();

                        insertStmt.executeBatch();

                        log.info(String.format("finish execute batch!!! took %d ms, size=%d", (new Date().getTime() - startTime.getTime()), k));
                    } catch (Exception e) {
                        errorInfo = "7777777777777777 execute batch failed! e=" + e;
                        log.error(errorInfo);

                        try {
                            conn.commit();
                        } catch (Throwable ee) {
                        }

                        batchProcessing = false;
                        JDBCUtil.close(insertStmt);
                        continue;
                    }
                }

                break;
            }

            JDBCUtil.close(insertStmt);
            JDBCUtil.close(stmt);
            conn.commit();
            log.info(" 6666666666666 commit()! size=" + searchResults.size());

            resultMap.put("conn", conn);

            return resultMap;
        } catch (Throwable e) {
            log.error(" processing failed!  e=" + e + " stacktrace = " + ExceptionUtils.getStackTrace(e));

            key = String.format("%d-%d-%s-dbconn", organizationId, repositoryId, dbConnKey);
            log.error("66666666666666 reset dbConn!!!");

            conn = (Connection) objectCache.get(key);
            JDBCUtil.close(conn);

            objectCache.put(key, null);

            throw e;
        } finally {
            //JDBCUtil.close(conn);            
        }
    }

    public static void writeDataToSQLDBWithPreparedStatmentFromSQLDB(boolean needEdfLastModifiedTime, Connection conn, List<Map<String, Object>> metadataDefinitions, String tableName, DatasourceConnection datasourceConnection, int organizationId, DataobjectType dataobjectType, List<Map<String, String>> sqldbRows, boolean isGMTTime, List<String> parentDataobjectPrimaryKeyNames) throws SQLException, Exception {
        String schema;
        String insertSql;
        String updateSql;
        String whereSql;
        String deleteSql;
        PreparedStatement insertStmt;
        PreparedStatement updateStmt;
        PreparedStatement deleteStmt;
        //Connection conn;
        Statement stmt = null;
        Map<String, Object> resultMap = new HashMap<>();
        int retry;

        DatabaseType databaseType;
        Map<String, String> columnLenPrecisionMap;
        List<String> primaryKeyNames;
        Map<String, String> tableColumnNameMap = new HashMap<>();
        String questionMarkStr;
        String columnNameStr;
        List<String> tableColumns;
        List<String> metadataColumns;
        String value;
        String insertSqlError;
        String updateSqlError;
        String deleteSqlError;
        String errorInfo;
        String key;
        List<String> addColumnSqlList;
        String tableColumnName;
        boolean found;
        int columnNo;
        boolean isPrimaryKey;
        Date date;
        int k = 0;
        boolean batchProcessing = true;

        try {

            schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
            databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);

            //key = String.format("%d-%d-%s-dbconn",organizationId,repositoryId,dbConnKey);
            //conn = (Connection)objectCache.get(key);
            //if ( conn != null )
            //{
            //    if ( conn.isClosed() )
            //        conn = null;
            //}

            /* if ( conn == null )
            {
                log.info ("geting new db connection ! key="+key);

                conn = JDBCUtil.getJdbcConnection(datasourceConnection);
                conn.setAutoCommit(false);

                objectCache.put(key, conn);
            }*/
            log.info(" db connection=" + conn + ", isclose=" + conn.isClosed());

            key = String.format("%d-primaryKeyNames", dataobjectType.getId());
            primaryKeyNames = (List<String>) objectCache.get(key);

            if (primaryKeyNames == null) {
                primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());

                if (parentDataobjectPrimaryKeyNames != null && !parentDataobjectPrimaryKeyNames.isEmpty()) {
                    primaryKeyNames.addAll(parentDataobjectPrimaryKeyNames);
                }

                objectCache.put(key, primaryKeyNames);

                log.info(" primarKeyNames size=" + primaryKeyNames.size() + " dataobject type name=" + dataobjectType.getName());
            }

            key = String.format("%d-columnLenPrecisionMap", dataobjectType.getId());
            columnLenPrecisionMap = (Map<String, String>) objectCache.get(key);
            if (columnLenPrecisionMap == null) {
                columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(dataobjectType.getMetadatas());
                objectCache.put(key, columnLenPrecisionMap);
            }

            Map<String, String> properties = Util.getDatasourceConnectionProperties(datasourceConnection.getProperties());
            String tableSpace = properties.get("table_space");
            String indexSpace = properties.get("index_space");

            tableColumns = new ArrayList<>();
            metadataColumns = new ArrayList<>();
            columnNameStr = "";
            questionMarkStr = "";

            for (Map<String, Object> definition : metadataDefinitions) {
                String name = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                tableColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(), name, true);

                tableColumns.add(tableColumnName);
                metadataColumns.add(name);
                columnNameStr += tableColumnName + ",";
                questionMarkStr += "?,";
                tableColumnNameMap.put(tableColumnName, name);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length() - 1);
            questionMarkStr = questionMarkStr.substring(0, questionMarkStr.length() - 1);

            insertSql = String.format("INSERT INTO %s ( %s ) VALUES (%s)", tableName, columnNameStr, questionMarkStr);

            while (true) {
                k = 0;

                insertStmt = conn.prepareStatement(insertSql);

                if (batchProcessing == false || primaryKeyNames.isEmpty()) {
                    stmt = conn.createStatement();
                }

                log.info("write to db .... size=" + sqldbRows.size());

                for (Map<String, String> row : sqldbRows) {
                    updateSql = String.format("UPDATE %s SET ", tableName);
                    whereSql = String.format("WHERE ");
                    deleteSql = String.format("DELETE from %s ", tableName);

                    int jj = 0;
                    int sqlType = 0;
                    for (String tableColumnStr : tableColumns) {
                        jj++;

                        if (needEdfLastModifiedTime && tableColumnStr.equals("edf_last_modified_time")) {
                            java.sql.Date currentDate = new java.sql.Date(new Date().getTime());
                            insertStmt.setDate(jj, currentDate);
                            continue;
                        }

                        String metadataName = tableColumnNameMap.get(tableColumnStr);

                        value = row.get(tableColumnStr.toUpperCase());

                        if (value == null) {
                            value = row.get(tableColumnStr.toLowerCase());
                        }

                        if (value == null) {
                            value = "";
                        } else {
                            value = value.trim();

                            value = Tool.removeSpecialChar(value);

                            if (!value.isEmpty() && Tool.isNumber(value)) {
                                value = Tool.normalizeNumber(value);
                            }
                        }

                        value = Tool.removeQuotation(value);

                        int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                        if (dataType == 0) {
                            dataType = 1;
                        }

                        if (dataType == MetadataDataType.DATE.getValue()) {
                            if (value.trim().isEmpty()) {
                                date = new Date(0);
                            } else {
                                date = new Date(Long.parseLong(value));
                            }

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToDateString1(date);
                        } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                            if (value.trim().isEmpty()) {
                                date = new Date(0);
                            } else {
                                date = new Date(Long.parseLong(value));
                            }

                            if (isGMTTime) {
                                date = Tool.changeFromGmt(date);
                            }

                            value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                        }

                        sqlType = MetadataDataType.findByValue(dataType).getSqlType();

                        Object obj = getValueObject(value, dataType);

                        log.debug(" tableColumStr=" + tableColumnStr + " dataType=" + dataType + " vaue=" + value);

                        if ((databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) && dataType == MetadataDataType.DATE.getValue()) {
                            java.sql.Date newDate = new java.sql.Date(((Date) obj).getTime());
                            insertStmt.setDate(jj, newDate);
                        } else if ((databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) && dataType == MetadataDataType.TIMESTAMP.getValue()) {
                            java.sql.Timestamp newTimestamp = new java.sql.Timestamp(((Date) obj).getTime());
                            insertStmt.setTimestamp(jj, newTimestamp);
                        } else if (value.length() >= 15 && (dataType == MetadataDataType.DOUBLE.getValue() || dataType == MetadataDataType.FLOAT.getValue())) {
                            BigDecimal bd = new BigDecimal(value);
                            insertStmt.setBigDecimal(jj, bd);
                        } else {
                            insertStmt.setObject(jj, obj, sqlType);
                        }
                    }

                    //insertSql = insertSql.substring(0,insertSql.length()-1)+")";
                    if (primaryKeyNames.isEmpty() || batchProcessing == false) {
                        columnNo = 0;

                        for (Map.Entry<String, String> entry : row.entrySet()) {
                            String metadataName = entry.getKey();
                            value = entry.getValue();

                            if (metadataName.equals("dataobjectId")) {
                                continue;
                            }

                            // if ( !metadataName.toLowerCase().startsWith(dataobjectType.getName().toLowerCase()) )                            
                            //    metadataName = dataobjectType.getName().trim()+"_"+metadataName;
                            columnNo++;

                            String tableColumnStr = Tool.changeTableColumnName(dataobjectType.getName().trim(), metadataName, true);

                            if (value == null || value.equals("null")) {
                                value = "";
                            } else {
                                value = value.trim();

                                value = Tool.removeSpecialChar(value);

                                if (!value.isEmpty() && Tool.isNumber(value)) {
                                    value = Tool.normalizeNumber(value);
                                }
                            }

                            value = Tool.removeQuotation(value);

                            int dataType = Util.getMetadataDataTypeString1(metadataName, metadataDefinitions, dataobjectType.getName());

                            if (dataType == 0) {
                                dataType = 1;
                            }

                            if (dataType == MetadataDataType.DATE.getValue()) {
                                if (value.isEmpty()) {
                                    date = new Date(0);
                                } else {
                                    date = new Date(Long.parseLong(value));
                                }

                                if (isGMTTime) {
                                    date = Tool.changeFromGmt(date);
                                }

                                value = Tool.convertDateToDateString(date);
                            } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                                if (value.isEmpty()) {
                                    date = new Date(0);
                                } else {
                                    date = new Date(Long.parseLong(value));
                                }

                                if (isGMTTime) {
                                    date = Tool.changeFromGmt(date);
                                }

                                value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                            }

                            // for updatesql
                            isPrimaryKey = false;

                            for (String kName : primaryKeyNames) {
                                String newColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(), metadataName, true);

                                if (kName.toLowerCase().equals(metadataName.toLowerCase()) || kName.toLowerCase().equals(newColumnName.toLowerCase())) {
                                    isPrimaryKey = true;
                                    break;
                                }

                                String kName1 = Tool.changeTableColumnName(dataobjectType.getName().trim(), kName, true);
                                if (kName1.toLowerCase().equals(newColumnName.toLowerCase())) {
                                    isPrimaryKey = true;
                                    break;
                                }
                            }

                            if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                                    || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                                    || dataType == MetadataDataType.DOUBLE.getValue()) {
                                if (value.isEmpty()) {
                                    String clp = columnLenPrecisionMap.get(metadataName);
                                    String defaultNullVal = Tool.getDefaultNullVal(clp);

                                    if (isPrimaryKey) {
                                        whereSql += String.format(" %s = '%s' AND", tableColumnStr, defaultNullVal);
                                    } else if (!tableColumnStr.equals("edf_id") || columnNo != 1) {
                                        updateSql += String.format(" %s = '%s',", tableColumnStr, defaultNullVal);
                                    }
                                } else if (isPrimaryKey) {
                                    whereSql += String.format(" %s = %s AND", tableColumnStr, value);
                                } else if (!tableColumnStr.equals("edf_id") || columnNo != 1) {
                                    updateSql += String.format(" %s = %s,", tableColumnStr, value);
                                }
                            } else if (isPrimaryKey) {
                                whereSql += String.format(" %s = '%s' AND", tableColumnStr, value);
                            } else if (!tableColumnStr.equals("edf_id") || columnNo != 1) {
                                updateSql += String.format(" %s = '%s',", tableColumnStr, value);
                            }
                        }

                        //if ( primaryKeyNames.isEmpty() ) // has no key
                        //{
                        //    whereSql += String.format(" edf_id='%s' AND",edfId);
                        //}
                        value = Tool.convertDateToTimestampStringWithMilliSecond1(new Date());

                        if (needEdfLastModifiedTime) {
                            updateSql += String.format(" edf_last_modified_time='%s'", value);
                        } else {
                            updateSql = Tool.removeLastChars(updateSql, 1);
                        }

                        //updateSql = updateSql.substring(0,updateSql.length()-1);
                        whereSql = whereSql.substring(0, whereSql.length() - 3);

                        updateSql += " " + whereSql;

                        deleteSql += whereSql;
                    }

                    retry = 0;
                    int ret = 0;

                    while (true) {
                        //log.info(" start to execute sql...");

                        retry++;

                        insertSqlError = "";
                        updateSqlError = "";
                        deleteSqlError = "";

                        /*  if ( primaryKeyNames.isEmpty() ) // no primary key
                        {
                            try
                            {
                                ret = stmt.executeUpdate(deleteSql);
                                //log.debug(" delete ret="+ret+" deleteSql="+deleteSql);
                                log.info(" delete ret="+ret);
                            }
                            catch(Exception e)
                            {
                                deleteSqlError  = "deleteSql failed! e="+e+" delete sql="+deleteSql;
                                log.error(deleteSqlError);

                                try{
                                    conn.commit();
                                }
                                catch(Exception ee){
                                }
                            }
                        }*/
                        try {
                            if (batchProcessing == true) {
                                insertStmt.addBatch();
                            } else {
                                ret = insertStmt.executeUpdate();
                                //log.debug("successfully insert ret="+ret+" insertSql="+insertSql);
                                log.info("k=" + k + " successfully insert single record! ret=" + ret);

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                    log.error("commit failed! e=" + ee);
                                }
                            }

                            k++;
                            break;
                        } catch (Exception e) {
                            insertSqlError = "insert database failed! e=" + e + " insert sql=" + insertSql;
                            //insertSqlError  = "insert database failed! e="+e;
                            //log.error(insertSqlError);

                            try {
                                conn.commit();
                            } catch (Exception ee) {
                            }

                            try {
                                ret = stmt.executeUpdate(updateSql);
                                //log.debug(" update ret="+ret+" updateSql="+updateSql);
                                log.info("k=" + k + " update single record. ret=" + ret);
                                if (ret == 1) {
                                    try {
                                        conn.commit();
                                    } catch (Exception ee) {
                                        log.error("commit failed! e=" + ee);
                                    }

                                    k++;
                                    break;
                                }

                                if (ret < 1) {
                                    log.info("7888888 update ret=" + ret + " updateSql=" + updateSql);
                                }
                            } catch (Exception ee) {
                                try {
                                    conn.commit();
                                } catch (Exception eee) {
                                }

                                if (updateSql.contains("SET WHERE")) // all column are keys
                                {
                                    break;
                                }

                                updateSqlError = "updateSql failed! e=" + ee + " update sql=" + updateSql;
                                //updateSqlError  = "updateSql failed! e="+ee;
                                log.error(updateSqlError);
                            }
                        }

                        errorInfo = insertSqlError + " " + updateSqlError;

                        log.error("1111retry=" + retry + ", errorInfo=" + errorInfo);

                        if (errorInfo.toLowerCase().contains("deadlock")) {
                            retry = 3;
                        } else if (errorInfo.toLowerCase().contains("column") || errorInfo.toLowerCase().contains("不存在") || errorInfo.toLowerCase().contains("not exist")
                                || errorInfo.toLowerCase().contains("doesn't exist") || primaryKeyNames.isEmpty()) {
                            log.info("4444 errorInfo=(" + errorInfo + ")");

                            try {
                                if (JDBCUtil.isTableExists(conn, schema, tableName)) {
                                    log.info("table name =" + tableName + " exists");

                                    List<String> existingColumnNames = JDBCUtil.getTableColumnNames(conn, schema, tableName.toLowerCase());
                                    log.info(" existing column size=" + existingColumnNames.size());

                                    for (Map<String, Object> definition : metadataDefinitions) {
                                        String metadataName = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                                        String description = (String) definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                        String dataType = (String) definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                        String lenStr = (String) definition.get(CommonKeys.METADATA_NODE_LENGTH);
                                        int len;

                                        if (lenStr == null || lenStr.trim().isEmpty()) {
                                            len = 0;
                                        } else {
                                            len = Integer.parseInt(lenStr);
                                        }

                                        log.info(" dataobject name=" + dataobjectType.getName() + " metadataName=" + metadataName);
                                        String newColumnName = metadataName.toLowerCase().replace(dataobjectType.getName().trim().toLowerCase() + "_", "");
                                        newColumnName = newColumnName.toUpperCase();

                                        log.info(" newColumnName=" + newColumnName);

                                        found = false;

                                        for (String existingColumnName : existingColumnNames) {
                                            if (newColumnName.equals(existingColumnName.toUpperCase())) {
                                                found = true;
                                                break;
                                            }
                                        }

                                        if (!found) {
                                            String sqlTypeStr = MetadataDataType.findByValue(Integer.parseInt(dataType)).getJdbcType();

                                            if (sqlTypeStr.contains("%d")) {
                                                sqlTypeStr = String.format(sqlTypeStr, len);
                                            }

                                            if (newColumnName.equals("EDF_LAST_MODIFIED_TIME")) {
                                                newColumnName = "edf_last_modified_time";
                                            }

                                            addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType, tableName, newColumnName, sqlTypeStr, description);

                                            try {
                                                for (String addColumnSql : addColumnSqlList) {
                                                    log.info("111 exceute add column sql =" + addColumnSql);
                                                    stmt.executeUpdate(addColumnSql);
                                                }
                                            } catch (Exception e) {
                                                log.error(" add column failed! e=" + e);
                                            }

                                            try {
                                                conn.commit();
                                            } catch (Exception ee) {
                                            }
                                        }
                                    }
                                } else {
                                    log.info("table name =" + tableName + " not exists");

                                    List<String> sqlList = Util.createTableForDataobjectType(false, true, dataobjectType.getName(), dataobjectType.getDescription(), tableName, "", primaryKeyNames, databaseType, datasourceConnection.getUserName(), metadataDefinitions, metadataColumns, columnLenPrecisionMap, tableSpace, indexSpace);

                                    try {
                                        for (String createTableSql : sqlList) {
                                            log.info(" exceute create table sql =" + createTableSql);
                                            stmt.executeUpdate(createTableSql);
                                        }

                                        conn.commit();
                                    } catch (Exception e) {
                                        log.error("2222 add column failed! e=" + e);

                                        try {
                                            conn.commit();
                                        } catch (Exception ee) {
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                errorInfo += " update/create table failed! try again ! e=" + e + " stacktrace=" + ExceptionUtils.getStackTrace(e);
                                log.error(errorInfo);

                                Tool.SleepAWhile(1, 0);

                                if (e instanceof JDBCConnectionException || e.getCause() instanceof JDBCConnectionException) {
                                    Util.resetPlatformEntityManagerFactory();
                                    Util.resetEntityManagerFactory(organizationId);
                                }

                                try {
                                    conn.commit();
                                } catch (Exception ee) {
                                }
                            }
                        }

                        if (retry >= 2 || stmt == null) {
                            try {
                                conn.rollback();
                            } catch (Exception e) {
                            }

                            //key = String.format("%d-%d-dbconn",organizationId,repositoryId);
                            //objectCache.put(key, null);
                            JDBCUtil.close(stmt);
                            JDBCUtil.close(insertStmt);

                            conn = null;
                            stmt = null;

                            log.error(" after retry " + retry + ", throw exception!");

                            throw new Exception(errorInfo);
                        }
                    }

                    if (k % 1000 == 0) {
                        log.info("2222222222 processed [" + k + "] items!");
                    }
                }

                if (batchProcessing == true) {
                    try {
                        Date startTime = new Date();

                        insertStmt.executeBatch();

                        log.info(String.format("finish execute batch!!! took %d ms, size=%d", (new Date().getTime() - startTime.getTime()), k));
                    } catch (Exception e) {
                        errorInfo = "7777777777777777 execute batch failed! e=" + e;
                        log.error(errorInfo);

                        try {
                            conn.commit();
                        } catch (Throwable ee) {
                        }

                        batchProcessing = false;
                        JDBCUtil.close(insertStmt);
                        continue;
                    }
                }

                break;
            }

            JDBCUtil.close(insertStmt);
            JDBCUtil.close(stmt);
            conn.commit();
            log.info(" 6666666666666 commit()! size=" + sqldbRows.size());
        } catch (Throwable e) {
            log.error(" processing failed!  e=" + e + " stacktrace = " + ExceptionUtils.getStackTrace(e));

            //key = String.format("%d-%d-%s-dbconn",organizationId,repositoryId,dbConnKey);
            //log.error("66666666666666 reset dbConn!!! key="+key);
            //conn = (Connection)objectCache.get(key);
            //JDBCUtil.close(conn);
            //objectCache.put(key, null);
            throw e;
        } finally {
            //JDBCUtil.close(conn);            
        }
    }

    public static Map<String, String> generateSql(List<Map<String, Object>> metadataDefinitions, String tableName, DatasourceConnection datasourceConnection, boolean needEdfIdField, int organizationId, int repositoryId, DataobjectType dataobjectType, Map<String, Object> result, boolean isGMTTime, String dateFormat, String datetimeFormat, boolean dataFromEs) throws SQLException, Exception {
        String schema;
        String insertSql = "";
        String updateSql = "";
        String whereSql = "";
        String deleteSql = "";
        String opType;

        DatabaseType databaseType;
        Map<String, String> columnLenPrecisionMap = null;
        List<String> primaryKeyNames;
        Map<String, String> tableColumnNameMap = new HashMap<>();
        String columnNameStr;
        List<String> tableColumns;
        List<String> metadataColumns;
        String value;
        String insertSqlError = "";
        String updateSqlError = "";
        String deleteSqlError = "";
        String errorInfo;
        String key;
        List<String> addColumnSqlList;
        String tableColumnName;
        boolean found;
        int columnNo;
        boolean isPrimaryKey;
        long dateValue;
        Date gmtDate;
        Date date;
        int k = 0;

        Map<String, String> resultMap = new HashMap<>();

        try {
            key = String.format("%d-schema", datasourceConnection.getId());
            schema = (String) objectCache.get(key);
            if (schema == null) {
                schema = JDBCUtil.getDatabaseConnectionSchema(datasourceConnection);
                objectCache.put(key, schema);
            }

            key = String.format("%d-databaseType", datasourceConnection.getId());
            databaseType = (DatabaseType) objectCache.get(key);
            if (databaseType == null) {
                databaseType = JDBCUtil.getDatabaseConnectionDatabaseType(datasourceConnection);
                objectCache.put(key, databaseType);
            }

            //log.info("conn isColosed="+conn.isClosed());
            key = String.format("%d-primaryKeyNames", dataobjectType.getId());
            primaryKeyNames = (List<String>) objectCache.get(key);
            if (primaryKeyNames == null) {
                primaryKeyNames = Util.getDataobjectTypePrimaryKeyFieldNames(dataobjectType.getMetadatas());
                objectCache.put(key, primaryKeyNames);
            }

            key = String.format("%d-columnLenPrecisionMap", dataobjectType.getId());
            columnLenPrecisionMap = (Map<String, String>) objectCache.get(key);
            if (columnLenPrecisionMap == null) {
                columnLenPrecisionMap = Util.getDataobjectTypeColumnLenPrecision(dataobjectType.getMetadatas());
                objectCache.put(key, columnLenPrecisionMap);
            }

            tableColumns = new ArrayList<>();
            metadataColumns = new ArrayList<>();
            columnNameStr = "";

            for (Map<String, Object> definition : metadataDefinitions) {
                String name = (String) definition.get(CommonKeys.METADATA_NODE_NAME);
                tableColumnName = Tool.changeTableColumnName(dataobjectType.getName().trim(), name, true);

                tableColumns.add(tableColumnName);
                metadataColumns.add(name);
                columnNameStr += tableColumnName + ",";
                tableColumnNameMap.put(tableColumnName, name);
            }

            if (needEdfIdField) {
                tableColumnName = "edf_id";
                tableColumns.add(tableColumnName);
                columnNameStr += tableColumnName + ",";
                tableColumnNameMap.put(tableColumnName, tableColumnName);
            }

            columnNameStr = columnNameStr.substring(0, columnNameStr.length() - 1);

            //stmt.setQueryTimeout(1000000);
            Map<String, Object> resultFields = (Map<String, Object>) result.get("fields");

            updateSql = String.format("UPDATE %s SET ", tableName);
            whereSql = String.format("WHERE ");
            deleteSql = String.format("DELETE from %s ", tableName);

            String edfId = (String) result.get("id");
            edfId = edfId.substring(0, 40);

            opType = (String) result.get("opType");

            //log.info("generate sql.... opType="+opType);
            if (opType.equals("I")) {
                insertSql = String.format("INSERT INTO %s ( %s ) VALUES (", tableName, columnNameStr);

                for (String tableColumnStr : tableColumns) {
                    String metadataName = tableColumnNameMap.get(tableColumnStr);

                    if (dataFromEs) {
                        value = String.valueOf(resultFields.get(metadataName));
                    } else {
                        value = String.valueOf(resultFields.get(tableColumnStr));
                    }

                    if (needEdfIdField && tableColumnStr.equals("edf_id")) {
                        value = edfId;
                    }

                    if (value == null || value.equals("null")) {
                        value = "";
                    } else {
                        value = value.trim();

                        value = Tool.removeSpecialChar(value);

                        if (!value.isEmpty() && Tool.isNumber(value)) {
                            value = Tool.normalizeNumber(value);
                        }
                    }

                    value = Tool.removeQuotation(value);

                    int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                    if (dataType == 0) {
                        dataType = 1;
                    }

                    if (dataType == MetadataDataType.DATE.getValue()) {
                        date = Tool.convertStringToDate(value, dateFormat);

                        if (isGMTTime) {
                            date = Tool.changeFromGmt(date);
                        }

                        value = Tool.convertDateToDateString1(date);
                    } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                        date = Tool.convertStringToDate(value, datetimeFormat);

                        if (isGMTTime) {
                            date = Tool.changeFromGmt(date);
                        }

                        value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                    }

                    if (databaseType == DatabaseType.ORACLE || databaseType == DatabaseType.ORACLE_SERVICE) {
                        if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                                || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                                || dataType == MetadataDataType.DOUBLE.getValue()) {
                            if (value.isEmpty()) {
                                String clp = columnLenPrecisionMap.get(metadataName);
                                String defaultNullVal = Tool.getDefaultNullVal(clp);

                                insertSql += String.format("'%s',", defaultNullVal);
                            } else {
                                insertSql += String.format("%s,", value);
                            }
                        } else if (dataType == MetadataDataType.DATE.getValue()) {
                            insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd'),", value);
                        } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                            insertSql += String.format("TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss'),", value.substring(0, 19));
                        } else {
                            insertSql += String.format("'%s',", value);
                        }
                    } else if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                            || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                            || dataType == MetadataDataType.DOUBLE.getValue()) {
                        if (value.isEmpty()) {
                            String clp = columnLenPrecisionMap.get(metadataName);
                            String defaultNullVal = Tool.getDefaultNullVal(clp);

                            insertSql += String.format("'%s',", defaultNullVal);
                        } else {
                            insertSql += String.format("%s,", value);
                        }
                    } else {
                        insertSql += String.format("'%s',", value);
                    }
                }

                insertSql = insertSql.substring(0, insertSql.length() - 1) + ")";

                String sqlKey = String.format("insert-%s", UUID.randomUUID().toString());
                resultMap.put(sqlKey, String.valueOf(dataobjectType.getId()) + " " + insertSql);
            } else if (opType.equals("U")) {
                columnNo = 0;

                for (Map.Entry<String, Object> entry : resultFields.entrySet()) {
                    String metadataName = entry.getKey();

                    if (dataFromEs) {
                        if (!metadataName.startsWith(dataobjectType.getName().trim().toUpperCase()) && !metadataName.startsWith(dataobjectType.getName().trim().toLowerCase())) {
                            continue;
                        }
                    } else {
                        metadataName = dataobjectType.getName().trim() + "_" + metadataName;
                    }

                    columnNo++;

                    String tableColumnStr = Tool.changeTableColumnName(dataobjectType.getName().trim(), metadataName, true);
                    //log.debug(" 2222 tableColumnStr="+tableColumnStr);

                    value = String.valueOf(entry.getValue());

                    if (needEdfIdField && tableColumnStr.equals("edf_id")) {
                        value = edfId;
                    }

                    if (value == null || value.equals("null")) {
                        value = "";
                    } else {
                        value = value.trim();

                        value = Tool.removeSpecialChar(value);

                        if (!value.isEmpty() && Tool.isNumber(value)) {
                            value = Tool.normalizeNumber(value);
                        }
                    }

                    value = Tool.removeQuotation(value);

                    int dataType = Util.getMetadataDataTypeString(metadataName, metadataDefinitions);

                    if (dataType == 0) {
                        dataType = 1;
                    }

                    if (dataType == MetadataDataType.DATE.getValue()) {
                        date = Tool.convertStringToDate(value, dateFormat);

                        if (isGMTTime) {
                            date = Tool.changeFromGmt(date);
                        }

                        value = Tool.convertDateToDateString(date);
                    } else if (dataType == MetadataDataType.TIMESTAMP.getValue()) {
                        date = Tool.convertStringToDate(value, datetimeFormat);

                        if (isGMTTime) {
                            date = Tool.changeFromGmt(date);
                        }

                        value = Tool.convertDateToTimestampStringWithMilliSecond1(date);
                    }

                    // for updatesql
                    isPrimaryKey = false;

                    for (String kName : primaryKeyNames) {
                        if (kName.toLowerCase().equals(metadataName.toLowerCase())) {
                            isPrimaryKey = true;
                            break;
                        }
                    }

                    if (dataType == MetadataDataType.BOOLEAN.getValue() || dataType == MetadataDataType.INTEGER.getValue()
                            || dataType == MetadataDataType.LONG.getValue() || dataType == MetadataDataType.FLOAT.getValue()
                            || dataType == MetadataDataType.DOUBLE.getValue()) {
                        if (value.isEmpty()) {
                            String clp = columnLenPrecisionMap.get(metadataName);
                            String defaultNullVal = Tool.getDefaultNullVal(clp);

                            if (isPrimaryKey) {
                                whereSql += String.format(" %s = '%s' AND", tableColumnStr, defaultNullVal);
                            } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                                updateSql += String.format(" %s = '%s',", tableColumnStr, defaultNullVal);
                            }
                        } else if (isPrimaryKey) {
                            whereSql += String.format(" %s = %s AND", tableColumnStr, value);
                        } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                            updateSql += String.format(" %s = %s,", tableColumnStr, value);
                        }
                    } else if (isPrimaryKey) {
                        whereSql += String.format(" %s = '%s' AND", tableColumnStr, value);
                    } else if (!needEdfIdField || !tableColumnStr.equals("edf_id") || columnNo != 1) {
                        updateSql += String.format(" %s = '%s',", tableColumnStr, value);
                    }
                }

                if (needEdfIdField && primaryKeyNames.isEmpty()) // has no key
                {
                    whereSql += String.format(" edf_id='%s' AND", edfId);
                }

                updateSql = updateSql.substring(0, updateSql.length() - 1);

                whereSql = whereSql.substring(0, whereSql.length() - 3);

                updateSql += " " + whereSql;

                String sqlKey = String.format("update-%s", updateSql.substring(0, updateSql.indexOf(" SET ")) + " " + whereSql);
                resultMap.put(sqlKey, String.valueOf(dataobjectType.getId()) + " " + updateSql);
            }

            return resultMap;
        } catch (Throwable e) {
            log.error(" generatesql failed!  e=" + e + " stacktrace = " + ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    public static String preProcessSQLLikeString(String sqlLikeString) {
        String str = sqlLikeString.replace("SELECT", "select");

        str = str.replace(" FROM ", " from ");
        str = str.replace(" WHERE ", " where ");
        str = str.replace(" GROUP ", " group ");
        str = str.replace(" ORDER ", " order ");
        str = str.replace(" BY ", " by ");
        str = str.replace(" COUNT(", " count(");
        str = str.replace(" SUM(", " sum(");
        str = str.replace(" AVG(", " avg(");
        str = str.replace(" MAX(", " max(");
        str = str.replace(" MIN(", " min(");
        str = str.replace(" AND ", " and ");
        str = str.replace(" OR ", " or ");
        str = str.replace(" LIMIT ", " limit ");

        return str;
    }

    public static String TimeToSecond(String date) {
        int count = 0;
        int result = 0;
        for (int i = 0; i < date.length() - 1; i++) {
            String a = String.valueOf(date.charAt(i));
            if (a.equals(":")) {
                count++;
            }
        }
        if (count == 1) {
            String min = date.substring(0, date.indexOf(":"));
            String sec = date.substring(date.indexOf(":") + 1, date.length());
            result = Integer.parseInt(min) * 60 + Integer.parseInt(sec);

        }
        if (count == 2) {
            String hh = date.substring(0, date.indexOf(":"));
            String min = date.substring(date.indexOf(":") + 1, date.indexOf(":") + 3);
            String sec = date.substring(date.indexOf(":") + 4, date.length());
            result = Integer.parseInt(hh) * 3600 + Integer.parseInt(min) * 60 + Integer.parseInt(sec);
            System.out.println(sec);
        }
        return String.valueOf(result);
    }
}
