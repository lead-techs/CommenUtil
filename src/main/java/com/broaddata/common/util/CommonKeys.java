/*
 * CoomonKeys.java
 *
 */

package com.broaddata.common.util;

public class CommonKeys
{
    public static int MIN_SEARCHABLE_PICTURE_AREA = 12000;
    public static int CAMERA_DATA_OBJECT_TYPE_ID = 12;
    public static int VIDEO_DATAOBJECT_TYPE_ID = 11;
    public static int PROCESS_TYPE_FOR_EXTRACT_VIDEO_OBJECT = 1;
    public static int CASE_CODE_DEFINITION_ID = 4839;
    
    public static final int ROW_NUMBER_OF_BIG_TABLE = 1000*10000;
    public static final int JDBC_QUERY_TIMEOUT = 30;
    public static final String KAFKA_CONNECTION_INFO_INDEX_TYPE = "kafka_connection_info";
    public static final String VIDEO_CONNECTION_INFO_INDEX_TYPE = "video_connection_info";
    public static final String SQLDB_SYNC_INFO_INDEX_TYPE = "sqldb_sync_info";
    public static final String RE_PROCESS_TIME_FIELD_INFO_INDEX_TYPE = "re_process_time_field_info";
    public static final int DATASERVICE_RETRY_TIMES = 3;
    public static final int KEEP_LIVE_SECOND = 600;
    public static final int MAX_DISPLAY_COLUMN_LEN = 70;
    public static final int MIN_DISPLAY_COLUMN_LEN = 10;
    
    public static final String CRITERIA_KEY_selectedMetadata = "selectedMetadata";
    public static final String CRITERIA_KEY_description = "description";    
    public static final String CRITERIA_KEY_dataType = "dataType";
    public static final String CRITERIA_KEY_selectedOperator= "selectedOperator";
    public static final String CRITERIA_KEY_operatorSelection = "operatorSelection";
    public static final String CRITERIA_KEY_valueSelection = "valueSelection";    
    public static final String CRITERIA_KEY_predefineRelativeTimeSelection = "predefineRelativeTimeSelection";
    public static final String CRITERIA_KEY_timeUnitValue = "timeUnitValue";
    public static final String CRITERIA_KEY_timeUnitSelection = "timeUnitSelection";
    public static final String CRITERIA_KEY_metadataFromValue = "metadataFromValue";
    public static final String CRITERIA_KEY_metadataToValue= "metadataToValue";
    public static final String CRITERIA_KEY_metadataValue = "metadataValue";
    public static final String CRITERIA_KEY_metadataFromValueDate = "metadataFromValueDate";
    public static final String CRITERIA_KEY_metadataToValueDate= "metadataToValueDate";
    public static final String CRITERIA_KEY_metadataValueDate = "metadataValueDate";    
    public static final String CRITERIA_KEY_singleInputMetadataValue = "singleInputMetadataValue";
    public static final String CRITERIA_KEY_selectedTargetDataobjectTypes = "selectedTargetDataobjectTypes";
    public static final String CRITERIA_KEY_selectPredefineRelativeTime = "selectPredefineRelativeTime";
    public static final String CRITERIA_KEY_selectRelativeTime = "selectRelativeTime";    
    public static final String CRITERIA_KEY_isRuntimeInput = "isRuntimeInput";    
    public static final String CRITERIA_KEY_selectFrom = "selectFrom"; 
    public static final String CRITERIA_KEY_searchBooleanOperatorType = "searchBooleanOperatorType"; 
        
    public static final String CRITERIA_KEY_dataobjectType = "dataobjectType";    
    public static final String CRITERIA_KEY_selectedDataobjectType = "selectedDataobjectType";
    
    public static final String DATASOURCE_CONNECTION_PROPERTIES = "//datasourceConnectionProperties/property"; 
    public static final String DATASOURCE_CONNECTION_PROPERTIES_NODE = "//datasourceConnectionProperties";
    public static final String DATASOURCE_CONNECTION_PROPERTY = "property";
    public static final String DATASOURCE_PROPERTIES = "//datasourceProperties/property";
      
    public static final int MAX_FETCH_NUMBER = 100000;
    public static final int SIZE_FOR_DATAVIEW_BATCH = 5000;
    public static final int JDBC_BATCH_COMMIT_NUMBER = 5000;
    public static final int FILE_BATCH_COMMIT_NUMBER = 10000;
    public static final int SCROLL_PAGE_SIZE = 2000;
    
    public static final String ELASTIC_SEARCH_NULL_VALUE = "null_value";
    public static final int DATASOURCE_ID_FOR_IMPORT_DATA_FROM_BROWSER = 0; //in seconds
    
    public static String INDEX_TYPE_FOR_CONTENT_INFO = "content_info_type";
    public static int DATA_CACHE_RETRY_NUMBER = 5;
    public static int DATA_CACHE_RETRY_WAITING_TIME_IN_SECONDS = 1;
    
    public static final String DATA_CACHE_KEY_DATAOBJECT = "dataobject_";
   //public static final String DATA_CACHE_KEY_DATAOBJECT_HEAD = "dataobject_head_";
   // public static final String DATA_CACHE_KEY_DATAOBJECT_BODY = "dataobject_body_";
    
    public static final int DATABASE_IS_TOO_SLOW_IN_MS = 5000; // 5000ms
    public static final int DATAOBJECT_NUMBER_IN_CACHE_UP_LIMIT = 50*10000;  // 40*10000
    public static final int DATAOBJECT_NUMBER_IN_CACHE_DOWN_LIMIT = 35*10000;  // 25*10000
    public static long MAX_SERVICE_LOG_LINE = 10*10000; // 30万
    public static int THRESHOLD_FOR_UNPROCESSED_DATAOBJECT_TO_WARN = 50000;
    public static int SYSTEM_ADMIN_ORGANIZATION_ID = 999999;
    public static String DATA_SERVICE_NOT_AVAILABLE = "data_service_not_available";
    public static int DATASOURCE_END_TASK_DATAOBJECT_BATCH_NUM = 600;
    public static int DATASOURCE_END_TASK_CLIENT_CONFIRM_NUM = 5000;
    public static int UPDATE_DB_RETRY_NUM  = 3;
    public static int THRIFT_RETRY_NUM = 10;
    public static String PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT = "processed_item_count";
    public static String PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN = "processed_item_time_taken";
    
    public static int LARGE_PANEL_HEIGHT = 500;
    public static int LARGE_PANEL_WIDTH = 900;
    public static int MEDIUM_PANEL_HEIGHT = 400;
    public static int MEDIUM_PANEL_WIDTH = 700;
    public static int PANEL_HEIGHT = 300;
    public static int PANEL_WIDTH = 500;
    public static String DATA_MART_KEY = "data_mart";
    public static long MAX_SIZE_PER_SHARD = 50*1000*1000*1000l; // 50G
    public static long MAX_COUNT_PER_SHARD = 1000*1000*1000l; // 1b
    public static int DEFAULT_SHARD_NUMBER_PER_INDEX = 5;
    public static int MAX_LENGTH_OF_SHOW_NAME = 25;
    public static String DATA_PROCESSING_EXTENSION_PACKAGE_NAME = "com.broaddata.dataprocessingextension";
    
    public static int JPA_LOCK_TIMEOUT = 30*1000; // 30 second 
    public static int DATA_SERVICE_RETRY_NUMBER = 2000;
    public static int DATA_SERVICE_RETRY_WAITING_TIME = 3*1000;  // 3 seconds
     
    public static final String SQLITE_URL_PREFIX = "jdbc:sqlite:";

    public static final String MOUNT_POINT = "/edf/data";
    public static final String BUNDLENAME = "edfresource";
    
    public static final int FILE_DATASOURCE_TYPE = 3; // for record file datasource
    public static final int FROM_BROWSER_DATASOURCE_TYPE = 10; 
    public static final String DATA_EVENT_YEAR = "data_event_year";
    public static final String DATA_EVENT_YEARMONTH = "data_event_yearmonth";
    public static final String DATA_EVENT_YEARMONTHDAY = "data_event_yearmonthday";
    
    public static final String ENCRYPTION_KEY = "12345678abcdefgh"; //must be 16 bytes
    public static final String DATA_STORE_NAME = "datastore";
    public static final String DATA_MART_NAME = "datamart";
    
    public static final int DATASOURCE_TYPE_FOR_FILE = 1;
    public static final int DATASOURCE_TYPE_FOR_DATABASE = 2;
    public static final int DATASOURCE_TYPE_FOR_RECORD_FILE =3;
    public static final int DATASOURCE_TYPE_FOR_HBASE =6;
    public static final int DATASOURCE_TYPE_FOR_KAFKA =8;
    
    public static final int DATAOBJECT_TYPE_FOR_FILE = 2;
    public static final int DATAOBJECT_TYPE_FOR_VIDEO_FILE = 11;
    public static final int DATAOBJECT_TYPE_FOR_VIDEO_OBJECT = 10;
    public static final int DATAOBJECT_TYPE_FOR_VIDEO_PICTURE = 13;
    public static final int DATAOBJECT_TYPE_FOR_RECORD_FILE = 3;
    public static final int DATAOBJECT_TYPE_FOR_PST_FILE = 6; // pst file
    public static final int DATAOBJECT_TYPE_FOR_CONVERGENCE = 7; 
    public static final int DATAOBJECT_TYPE_FOR_DATA_QUALITY_CHECK_RESULT = 8; 
    public static final int DATAOBJECT_TYPE_FOR_DATA_QUALITY_CHECK_RESULT_SUMMARY = 9; 
    
    public static final String FIELD_SEPERATOR = "~";
    public static final String LOG_EVENT_TIME="log_event_time";
    public static final int MAX_EXMAPLE_FILE_READ_LINE = 100;

    public static final int DEFAULT_DATAOBJECT_TYPE_CATALOG = 2;
    
    public static final int ROOT_DATAOBJECT_TYPE = 1;
    public static final int LOG_DATAOBJECT_TYPE_CATALOG = 1;
    public static final int ROOT_DATAOBJECT_TYPE_ID_FOR_LOG = 4;
    public static final int ROOT_DATAOBJECT_TYPE_ID_FOR_METRICS = 5;
    public static final int DATASOURCE_TYPE_LOG_SERVER = 4; 
    public static final int DATASOURCE_TYPE_VIDEO_FILE_SERVER = 9; 
    
    public static final int DATAOBJECT_TYPE_CATEGORY_ID_FOR_METRICS=6;
    
    public static final int MAX_EXPECTED_SEARCH_HITS = 9999;
    public static final int MAX_EXPECTED_EXCUTION_TIME = 300*60*1000;  // 300 mins
    
    public static final int DB_LOCK_TIMEOUT = 10; // 10 milli seconds

    public static final String COMMON_SERVICE_IP = "common.service.ip";
    public static final String DEFAULT_ENCODING = "UTF-8";
    public static final String NODE_CONFIG = "../conf/node.properties";    
    public static final String SYSLOG_SERVER_CONFIG = "../conf/syslogserver.properties";  
    
    public static final String COMMON_SERVICE_LOG4J_CONFIG = "../conf/logconf/commonservice.log4j.properties"; 
    public static final String WORK_SCHEDULER_SERVICE_LOG4J_CONFIG = "../conf/logconf/workscheduler.log4j.properties"; 
    public static final String SERVICE_GUARD_LOG4J_CONFIG = "../conf/logconf/serviceguard.log4j.properties"; 
    
    public static final String DATAOBJECT_TYPE_CREATOR_LOG4J_CONFIG = "../conf/logconf/dataobjecttypecreator.log4j.properties"; 
    public static final String DATA_SERVICE_LOG4J_CONFIG = "../conf/logconf/dataservice.log4j.properties"; 
    public static final String IMAGE_SERVICE_LOG4J_CONFIG = "../conf/logconf/imageservice.log4j.properties"; 
    public static final String SEARCH_SERVICE_LOG4J_CONFIG = "../conf/logconf/searchservice.log4j.properties";     
    public static final String INDEX_SERVICE_LOG4J_CONFIG = "../conf/logconf/indexservice.log4j.properties"; 
    public static final String RETENTION_SERVICE_LOG4J_CONFIG = "../conf/logconf/retentionservice.log4j.properties";    
    public static final String DATA_PROCESSING_SERVICE_LOG4J_CONFIG = "../conf/logconf/dataprocessingservice.log4j.properties";    
    public static final String DATA_WORKER_SERVICE_LOG4J_CONFIG = "../conf/logconf/dataworkerservice.log4j.properties";   
    public static final String SYSLOG_SERVER_LOG4J_CONFIG = "../conf/logconf/syslogserver.log4j.properties";   
    public static final String EDF_TOOL_LOG4J_CONFIG = "../conf/logconf/edfTools.log4j.properties";
    public static final String VIDEO_WEBPLAY_LOG4J_CONFIG = "../conf/logconf/videowebplay.log4j.properties";
                 
    public static final String METADATA_INIT_XML  = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+"\n"+"<dataobjectMetadatas>\n"+"</dataobjectMetadatas>"; 
    public static final String METADATA_NODES = "//dataobjectMetadatas";
    public static final String METADATA_NODE = "//dataobjectMetadatas/metadata";
    public static final String METADATA_NODE_METADATA = "metadata";
    public static final String METADATA_NODE_NAME = "name";
    public static final String METADATA_NODE_DESCRIPTION = "description";
    public static final String METADATA_NODE_DATA_ITEM_STANDARD_ID = "dataItemStandardId";
    public static final String METADATA_NODE_METADATA_GROUP = "metadataGroup";
    public static final String METADATA_NODE_DATA_TYPE = "dataType";
    public static final String METADATA_NODE_INDEX_TYPE = "indexType"; 
    public static final String METADATA_NODE_IS_PRIMARY_KEY = "isPrimaryKey";
    public static final String METADATA_NODE_IS_SINGLE_VALUE = "isSingleValue";
    public static final String METADATA_NODE_IN_SEARCH_RESULT = "inSearchResult"; 
    public static final String METADATA_NODE_FLAGS = "flags"; 
    public static final String METADATA_NODE_SEARCH_FACTOR = "searchFactor";
    public static final String METADATA_NODE_SELECT_FROM = "selectFrom";   
    public static final String METADATA_NODE_LENGTH = "length";       
    public static final String METADATA_NODE_PRECISION = "precision";  
    public static final String METADATA_NODE_SHOW_POSITION = "showPosition";  
    public static final String METADATA_NODE_IS_DEFAULT_TO_SHOW = "isDefaultToShow";     
    public static final String METADATA_NODE_SELECTED_CODE_ID = "selectedCodeId";   
    public static final String METADATA_DATETIME_FORMAT= "datetimeFormat";
        
    public static final long INTRANET_BIG_FILE_SIZE = 500 * 1024 * 1024; // more than 500M is big file over intranet
    public static final long INTERNET_BIG_FILE_SIZE = 5 * 1024 * 1024; // more than 5M is big file over internet
    public static final int FILE_BUFFER_SIZE = 8 * 1024; // more than 100M is big file
    public static final int MAX_METADATA_LENGTH = 60*1000; // 60k
    
    public static final int NUMBER_OF_INDEX_PER_NODE = 500;
    
    public static final int RECOMMENDATION_SERVICE_DEFULT_ITEM_QTY = 10;
            
    public static final String JTA_PLATFORM_DATABASE_NAME = "jta.edf_platform";
    public static final String JTA_ORGANIZATION_DATABASE_NAME = "jta.edf_organization";
    public static final String PLATFORM_DATABASE_NAME = "edf_platform";
    public static final String ORGANIZATION_DATABASE_NAME = "edf_organization";  
    public static final String REC_DATABASE_NAME = "edf_rec";
    
    public static final String INDEX_REQUEST = "IndexRequest";
    public static final String TO_SQLDB_REQUEST = "ToSqldbRequest";
    public static final String TO_HBASE_REQUEST = "ToHbaseRequest";
    public static final String VIDEO_ANALYSIS_EVENT = "VideoAnalysisEvent";
    
    public static final String DATA_OPERATION_REQUEST = "DataOperationRequest";
    public static final String SEARCH_REQUEST = "SearchRequest";
    public static final String RETRIEVAL_REQUEST = "RetrievalRequest";
    public static final String RETRIEVAL_RESPONSE = "RetrievalResponse";
    public static final String DATA_PROCESS_REQUEST = "DataProcessRequest";

    public static final String JMX_DOMAIN = "com.broaddata.edf";
    public static final String JMX_TYPE_EDF_NODE = "edfNode";
    public static final String JMX_TYPE_SERVICE_NODE = "serviceNode";
    public static final String JMX_TYPE_MANAGEMENT_NODE = "managementNode";
    public static final String JMX_TYPE_DATA_COLLECTION_NODE = "dataCollectingNode";
        
    public static final int THRIFT_DATA_SERVICE_PORT = 8099;
    public static final int THRIFT_COMMON_SERVICE_PORT = 8091;
    public static final int THRIFT_IMAGE_SERVICE_PORT = 8098;
    public static final int THRIFT_RECOMMENDATION_SERVICE_PORT = 8096;
    
    public static final int THRIFT_SERVICE_GUARD_PORT = 8092; 
    public static final int THRIFT_SERVICE_GUARD_PORT_FOR_SERVICE_NODE = 8093;    
    public static final int THRIFT_SERVICE_GUARD_PORT_FOR_MANAGEMENT_NODE = 8094;    
    public static final int THRIFT_SERVICE_GUARD_PORT_FOR_DATA_COLLECTING_NODE = 8095;
    
    public static final int THRIFT_TIMEOUT = 10*60*1000; // 10 minutes
    
    public static final int DATA_WORKER_FREQUENCY_OF_CHECKING_SERVER_JOB = 5; //in seconds
    public static final int DATA_WORKER_FREQUENCY_OF_CHECKING_CREATE_KAFKA_PROCESSOR = 30; //in seconds
           
    public static final int NUMBER_OF_DATA_ROUTING_THREAD = 4;
    public static final int DATA_ROUTING_PROCESSOR_FREQUENCY_OF_CHECKING = 3; //in seconds
    public static final int MAX_NUMBER_OF_DATA_PER_TIME = 10;

    public static final int NUMBER_OF_RETENTION_SERVICE_THREAD = 4; // default 4
    public static final int DATA_WORKER_SERVICE_NUMBER_OF_WORKING_THREAD = 6;  // default 10
    public static final int NUMBER_OF_DATAOBJECT_INDEX_THREAD = 12; // default 12
    public static final int NUMBER_OF_SQLDB_SYNC_THREAD = 8; // default 4
    public static final int NUMBER_OF_ES_SYNC_THREAD = 8; // default 4
    public static final int NUMBER_OF_HBASE_SYNC_THREAD = 4; // default 4
    public static final int NUMBER_OF_MESSAGE_HUB_PROCESSOR_THREAD = 4; //
    public static final int NUMBER_OF_DATAOBJECT_CLEAN_THREAD = 4; // default 4
    public static final int NUMBER_OF_RETENTION_THREAD = 1;  // default 36
    public static final int NUMBER_OF_DATA_BATCH_PROCESSING_THREAD = 8;  // default 8
    public static final int RETENTION_PROCESSOR_FREQUENCY_OF_CHECKING_UNPROCESSED_DOCUMENT = 2; //in seconds
    public static final int INDEX_PROCESSOR_FREQUENCY_OF_CHECKING_UNPROCESSED_DOCUMENT = 10; //in seconds
    
    public static final int CHECK_HEALTH_FREQUENCY = 60; //in seconds
    public static final int CHECK_SERVICE_FREQUENCY = 60; //in seconds
    public static final int CHECK_UPGRADE_FREQUENCY = 30; //in seconds
    public static final int CHECK_REINDEX_FREQUENCY = 3; //in seconds
    public static final int CHECK_SERVICE_LOG = 60*60*12; //in seconds 60*60
    
    public static final int BATCH_PROCESSOR_FREQUENCY_OF_CHECKING_UNPROCESSED_DOCUMENT = 5; //in seconds
    public static final int MAX_NUMBER_OF_INDEXING_DATAOBJECT_PER_TIME = 30; // default 100
    public static final int MAX_NUMBER_OF_DELETING_DATAOBJECT_PER_TIME = 30;
    public static final int MAX_NUMBER_OF_PROCESS_REPORT_PER_TIME = 5;
    public static final int MAX_NUMBER_OF_PROCESS_ALERT_PER_TIME = 5;
     
    public static final String CONTENT_LOCATION_BLOB_STORE = "blobstore";
    public static final String CONTENT_LOCATION_FILE_SERVER = "fileserver";
    public static final String CONTENT_LOCATION_WEB = "web";
    public static final String CONTENT_LOCATION_DATABASE = "database";
    
    public static final String SERVICE_PROPERTIES = "//config/property";
    
    public static final String DATASOURCE_NODE = "//taskConfig/datasources/datasourceId";
    public static final String ARCHIVE_FILE_NODE = "//taskConfig/datasources/archiveFile";
    public static final String DATASOURCES_NODE = "//taskConfig/datasources";
    public static final String DATASOURCE_NODE_NAME = "datasource";
    public static final String DATASOURCE_ID = "datasourceId";
    public static final String ARCHIVE_NODE_NAME = "archiveFile";
     
    public static final int MAX_LINE_PER_JOB = 10*10000*10000; // 10亿
    public static final long MAX_SIZE_PER_JOB = 10*1000000; // 10M
    public static final long MAX_DOCUMENT_NUMBER_PER_JOB = 100;
    
    public static final int NUMBER_OF_THREAD_OF_SEARCH_SERVICE_PER_NODE = 12;
    public static final int NUMBER_OF_THREAD_OF_RETRIEVAL_SERVICE_PER_NODE = 1;
            
    public static final int ELASTICSEARCH_RETRY = 3; // 3 times
    public static final int ELASTICSEARCH_WAIT = 500; // half second
        
    public static final String OSS_ENDPOINT = "http://oss.aliyuncs.com";
    public static final String OSS_ACCESS_ID = "R9Hzm6APgovT81kR";
    public static final String OSS_ACCESS_KEY = "ha2Xx0XIZtS7qO55Fi25vp3itYgPlx";
    public static final String OSS_BUCKETNAME = "frank-test";
}
