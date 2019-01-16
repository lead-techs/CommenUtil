/*
 * KafkaProcessor.java
 */

package com.broaddata.common.processor;

import org.apache.log4j.Logger;
import java.util.Properties;
//import kafka.consumer.ConsumerConfig;
//import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.lang3.exception.ExceptionUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
//import kafka.consumer.Consumer;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Calendar;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.broaddata.common.thrift.dataservice.CreateKafkaConnectionJob;
import com.broaddata.common.thrift.dataservice.DataService;
import com.broaddata.common.util.Tool;
import com.broaddata.common.util.DataServiceConnector;
import com.broaddata.common.model.platform.ComputingNode;
import com.broaddata.common.util.CommonKeys;
import com.broaddata.common.util.JDBCUtil;
import com.broaddata.common.util.MessageProcessor;
import com.broaddata.common.model.organization.DatasourceConnection;
import com.broaddata.common.model.platform.DataobjectType;
import com.broaddata.common.model.enumeration.DatabaseType;
import com.broaddata.common.model.enumeration.MetadataDataType;
import com.broaddata.common.util.Counter;
import org.apache.kafka.clients.consumer.Consumer;
 
public class KafkaProcessor implements Runnable
{
    static final Logger log = Logger.getLogger("KafkaProcessor");

    public static final int KAFKA_MESSAGE_STOP_SIZE = 100000;
    public static final int KAFKA_MESSAGE_COMMIT_SIZE = 10000;
    public static final int NUMBER_OF_WRITE_DB_THREAD = 5;
    public static final int NUMBER_OF_SQL_CACHE = 3;
    
    private String connectionUrl = "" ;
    private String consumerGroup = "";
    private String topic = "";
    private volatile DataServiceConnector dsConn;
    private String dataServiceIps;
    private CreateKafkaConnectionJob job;
    private ComputingNode computingNode;
    private ScheduledExecutorService schedulerForDatasourceEndTask = Executors.newScheduledThreadPool(1);  
    private volatile List<Map<String,String>> insertSqlCacheList = new ArrayList<>();
    private volatile List<Map<String,String>> updateSqlCacheList = new ArrayList<>();
    private final BlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(KAFKA_MESSAGE_STOP_SIZE+1000);
    private final BlockingQueue<String> sqlCacheReadyQueue = new ArrayBlockingQueue<>(1);
    private volatile boolean stopReadFromKafka = false;
    private volatile String processingInfo = "";
    private volatile String errorInfo = "";
    private int clientNodeId;
    private volatile Map<String,String> processingInfoMap = new HashMap<>();
    private Counter serviceCounter;
   
    public KafkaProcessor(String dataServiceIps,CreateKafkaConnectionJob job,ComputingNode computingNode,String connectionUrl,String consumerGroup,String topic,int clientNodeId,Counter serviceCounter) 
    {
        this.dataServiceIps = dataServiceIps;
        this.connectionUrl = connectionUrl;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.job = job;
        this.computingNode = computingNode;
        this.clientNodeId = clientNodeId;
        this.serviceCounter = serviceCounter;
    }

    @Override
    public void run()
    {
        try
        {
            MessageConsumer messageConsumer = new MessageConsumer(dataServiceIps,connectionUrl,consumerGroup,topic);

            String[] vals = job.getMessageParameters().split("\\;");
            String datasourceConnectionName = vals[0]; // database name

            List<String> skipedMessageNames = new ArrayList<>(); 
            if ( vals.length > 1)
            {
                String skipedMessageNamesStr = vals[1];
                String[] vals1 = skipedMessageNamesStr.split("\\,");

                for(String val : vals1)
                    skipedMessageNames.add(val);
            }

            for(int i=0;i<NUMBER_OF_SQL_CACHE;i++)
            {
                insertSqlCacheList.add(new HashMap<String,String>());
                updateSqlCacheList.add(new HashMap<String,String>());
            }
            
            MessageProcessor messageProcessor = new MessageProcessor(job.getOrganizationId(), job.getRepositoryId(),job.getSourceApplicationId(),job.getMessageType(),datasourceConnectionName,skipedMessageNames,clientNodeId);               
            MessageHandler messageHandler = new MessageHandler(dataServiceIps,messageConsumer,messageProcessor,job);
            new Thread(messageHandler).start(); 

            SQLHandler sqlHandler = new SQLHandler(dataServiceIps,job);
            new Thread(sqlHandler).start(); 
            
            messageConsumer.processMessage(messageHandler,sqlHandler);
        }
        catch(Exception e)
        {
            log.error(" KafkaProcessor.run() failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));    
        }
    }
             
    class MessageConsumer
    {
        //private ConsumerConnector kafkaConsumer;
        private String topic;
        private String connectionUrl;
        private String consumerGroup;
        private Properties props;
        private ScheduledExecutorService schedulerForDatasourceEndTask = Executors.newScheduledThreadPool(1);
        private DataServiceConnector dsConn = new DataServiceConnector();
        private String dataServiceIps;
        private volatile Date startToPendingOnKafkaTime;
        
        public MessageConsumer(String dataServiceIps,String connectionUrl,String consumerGroup,String topic)
        {
            this.topic = topic;
            this.connectionUrl = connectionUrl;
            this.consumerGroup = consumerGroup;
            this.dataServiceIps = dataServiceIps;
        }

        public Date getStartToPendingOnKafkaTime() {
            return startToPendingOnKafkaTime;
        }
    
      /*  public ConsumerConnector getKafkaConsumer() {
            return kafkaConsumer;
        }
       
        public ConsumerConnector getConsumer()
        {
            return kafkaConsumer;    
        }        */
        
        public boolean isStopReadFromKafka() {
            return stopReadFromKafka;
        }
        
      /*  private ConsumerConfig createConsumerConfig(String zookeeper,String groupId) 
        {
            Properties props = new Properties();

            props.setProperty("auto.offset.reset","smallest");
            props.put("zookeeper.connect", zookeeper);
            props.put("group.id", groupId);
            props.put("auto.commit.enable", "false");
            
            //props.put("enable.auto.commit", "true");
            //props.put("auto.commit.interval.ms", "1000");
  
            return new ConsumerConfig(props);
	}*/

        public void processMessage(MessageHandler messageHandler,SQLHandler sqlHandler) throws Exception
        {
            long k = 0;
            String message;
  
            while(true)
            {
                try
                {
                    dsConn.getClient(dataServiceIps);

                    log.info(" 111 creating consumer !");
              //      kafkaConsumer = Consumer.createJavaConsumerConnector(createConsumerConfig(connectionUrl,consumerGroup));

                    log.info("start process kafka message .... topic="+topic+ " dsConn="+dsConn);

                    Map<String, Integer> topicCountMap = new HashMap<>();
                    topicCountMap.put(topic,1);

                    log.info("creating message streams !!!");

                   // Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer.createMessageStreams(topicCountMap);

                    log.info("after create messageStreams!"); 

                   // KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
                   // ConsumerIterator<byte[], byte[]> it = stream.iterator();

                   // log.info("111 read consumer iterator ="+it);

             //       schedulerForDatasourceEndTask.scheduleWithFixedDelay(new StatusChecker(this,kafkaConsumer,dsConn,messageHandler,sqlHandler), 0, 60, SECONDS);

                    stopReadFromKafka = false;
                    messageQueue.clear();

                    startToPendingOnKafkaTime = new Date();
                         
                   /* while( it.hasNext() )
                    {
                        message = new String(it.next().message(),"utf-8");

                        k++;

                        log.debug("recieved message from kafka, k="+k+" topic="+job.getTopic());

                        messageQueue.put(message);

                        if ( k%KAFKA_MESSAGE_STOP_SIZE == 0 )
                            stopReadFromKafka = true;
                        else
                        {
                            Date newDate = Tool.dateAddDiff(new Date(), -1, Calendar.MINUTE);

                            if ( newDate.after(startToPendingOnKafkaTime) )
                            {
                                log.info(" pending on kafka more than 1 minute! StartToPendingOnKafkaTime="+startToPendingOnKafkaTime+" newDate="+newDate);
                                stopReadFromKafka = true;
                            }
                        }
                        
                        while( stopReadFromKafka )
                        {
                            log.debug("stopReadFromKafka is true. queue size="+messageQueue.size()+"  sqlCacheReadyQueue.size()="+sqlCacheReadyQueue.size());
                            Tool.SleepAWhileInMS(500);
                                   
                            if (messageQueue.size() > 0 || sqlCacheReadyQueue.size() > 0)
                                continue;
                                                
                            if ( messageHandler.hasRemainingData() == false && sqlHandler.hasRemainingData() == false )
                            {
                                log.info("111111111111 commit kafka !!!!! k="+k);
                                kafkaConsumer.commitOffsets();
                                stopReadFromKafka = false;
                            }
                        }

                        startToPendingOnKafkaTime = new Date();
                        //log.info("read kafka message ..... message in queue="+messageQueue.size()+" topic="+job.getTopic());
                    }*/
                }
                catch (Exception e)
                {
                    log.error("consume kafka message failed! e="+e+" statcktrace="+ExceptionUtils.getStackTrace(e));
                    throw e;
                }
                finally
                {                
            //        if ( kafkaConsumer != null )
             //            kafkaConsumer.shutdown();
                    
                    if ( dsConn != null )
                        dsConn.close();
                }
            }        
        }    
    }
    
    class StatusChecker implements Runnable 
    {       
        private DataServiceConnector dsConn;
        private MessageConsumer messageConsumer;
       // private ConsumerConnector kafkaConsumer;
        private MessageHandler messageHandler;
        private SQLHandler sqlHandler;       
                
/*        public StatusChecker(MessageConsumer messageConsumer,ConsumerConnector kafkaConsumer,DataServiceConnector dsConn,MessageHandler messageHandler,SQLHandler sqlHandler) 
        {
            this.dsConn = dsConn;
            this.messageConsumer = messageConsumer;
            this.kafkaConsumer = kafkaConsumer;
            this.messageHandler = messageHandler;
            this.sqlHandler = sqlHandler;
        }
  */      
        @Override
        public void run()
        {
            try
            {
                log.info(" update status when pending on kafka reading! topic="+job.topic);
                String processingInfo = processingInfoMap.get("processingInfo");
                if ( processingInfo == null )
                    processingInfo = "";
                
                dsConn.getClient().updateKafkaConnectionLatestStatus(job.getOrganizationId(), job.getRepositoryId(), job.getDatasourceId(), computingNode.getId(),processingInfoMap.get("lastProcessedMessage"),processingInfo);
            }
            catch(Exception e)
            {
                log.error(" dataService.updateKafkaConnectionLatestStatus() failed! e="+e);
                dsConn.reConnect();
            }
            
         /*   try
            {                           
                if (messageQueue.size() > 0 || sqlCacheReadyQueue.size() > 0)
                {
                    log.warn("queue size is bigger than 0.  queue size="+messageQueue.size()+"  sqlCacheReadyQueue.size()="+sqlCacheReadyQueue.size());
                    return;
                }       
                        
                if ( messageConsumer.isStopReadFromKafka() == true )
                {
                    log.info("reading from kafka is stopped!!! ");
                    return;
                }
                
                Date date = messageConsumer.getStartToPendingOnKafkaTime();
                Date newDate = Tool.dateAddDiff(new Date(), -2, Calendar.MINUTE);

                if ( newDate.before(date) )
                {
                    log.info(" pending on kafka lessage than 2 minute! StartToPendingOnKafkaTime="+date);
                    return;
                }
                        
                if ( messageHandler.hasRemainingData() == false && sqlHandler.hasRemainingData() == false  )
                {
                    log.info("222222222222 commit kafka !!!!!");
                    kafkaConsumer.commitOffsets();
                }
            }
            catch(Exception e)
            {
                log.error(" processing commit () failed! e="+e);
            }*/
        }
    }

    class MessageHandler implements Runnable
    {
        MessageConsumer messageConsumer;
        MessageProcessor messageProcessor;
        private int currentPosition = 0 ;
        private Map<String,String> insertSqlCache = new HashMap<>();
        private Map<String,String> updateSqlCache = new HashMap<>();
        private CreateKafkaConnectionJob job;
        private DataService.Client dataService = null;
        private DataServiceConnector dsConn = new DataServiceConnector();
        private String dataServiceIps;
        private volatile boolean hasRemainingData = true;
                
        public MessageHandler(String dataServiceIps,MessageConsumer messageConsumer,MessageProcessor messageProcessor,CreateKafkaConnectionJob job) 
        {
            this.messageConsumer = messageConsumer;
            this.messageProcessor = messageProcessor;
            this.job = job;
            this.dataServiceIps = dataServiceIps;
        }
        
        public boolean hasRemainingData()
        {
            return hasRemainingData;
        }
        
        @Override
        public void run()
        {
            long lastCurrentNo = 0;
            long currentNo = 0;
            long totalNumber = 0;
            Date startTime = new Date();
            String message;
            long noDataTimes = 0;
            Date date1 = new Date();

            try
            {        
                dataService = dsConn.getClient(dataServiceIps);

                insertSqlCache = insertSqlCacheList.get(currentPosition);
                insertSqlCache.clear();

                updateSqlCache = updateSqlCacheList.get(currentPosition);
                updateSqlCache.clear();

                messageProcessor.setSqlCache(insertSqlCache,updateSqlCache);
                processingInfoMap.clear();            
 
                while(true)
                {
                    try
                    {
                        log.info(" update status before reading from queue, topic="+job.topic);
                        processingInfoMap.put("processingInfo", "start to read from queue!");
                        dataService.updateKafkaConnectionLatestStatus(job.getOrganizationId(), job.getRepositoryId(), job.getDatasourceId(), computingNode.getId(),"",processingInfoMap.get("processingInfo"));
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error("11111111111111111 updateKafkaConnectionLatestStatus() failed! e="+e);
                        dsConn.reConnect();
                        dataService = dsConn.getClient();

                        Tool.SleepAWhileInMS(500);
                    }
                }

                while(true)
                {
                    //log.info("read from queue ...  topic="+job.topic);

                    if ( currentNo < KAFKA_MESSAGE_COMMIT_SIZE )
                        message = messageQueue.poll();
                    else
                        message = null;

                    if ( message == null )
                    {
                        log.debug("no more message or > commit size, start to write to db ...  topic="+job.topic+" currentNo="+currentNo+" message queue size="+messageQueue.size());

                        if ( currentNo > 0 )
                        {
                            if ( insertSqlCache.isEmpty() && updateSqlCache.isEmpty() )
                            {
                                log.info(" insertSqlCache  and updateSqlCache are empty");
                                Tool.SleepAWhileInMS(200);
                                continue;
                            }

                            //log.info(String.format(" generate sql finished! size=%d, took %d ms",currentNo,(new Date().getTime()-date1.getTime())));

                            String sqlInfo = String.format(" generate sql finished! size=%d, took %d ms",currentNo,(new Date().getTime()-date1.getTime()));
                            log.info(sqlInfo);
                            
                            log.info("1111 put to sqlCacheReadyQueue!  currentPosition="+currentPosition);
                            sqlCacheReadyQueue.put(String.valueOf(currentPosition));
                            log.info("2222 finish put to sqlCacheReadyQueue!  currentPosition="+currentPosition);
                            
                            currentPosition++;
                            
                            if ( currentPosition == NUMBER_OF_SQL_CACHE )
                                currentPosition = 0;
                            
                            insertSqlCache = insertSqlCacheList.get(currentPosition);
                            insertSqlCache.clear();
                            
                            updateSqlCache = updateSqlCacheList.get(currentPosition);
                            updateSqlCache.clear();
                        
                            messageProcessor.setSqlCache(insertSqlCache,updateSqlCache);                            
                                     
                            long saving = currentNo + lastCurrentNo;
                            long saved = totalNumber - saving;
                                                        
                            long timeSpentInSecond = (new Date().getTime()-startTime.getTime())/1000;
                            long messagesPerSecond = 0;

                            if ( timeSpentInSecond == 0 )
                                messagesPerSecond = 0;
                            else
                                messagesPerSecond = saved/timeSpentInSecond;
                            
                            String processingInfo = "recieved total="+totalNumber+",saving="+saving+",saved="+saved+", ["+messagesPerSecond+"]per second, "+sqlInfo+", start time="+startTime;
                                                                
                            processingInfoMap.put("processingInfo", processingInfo);
                            
                            lastCurrentNo = currentNo;
                            currentNo = 0;
                                                        
                            date1 = new Date();
                            
                            continue;
                        }

                        hasRemainingData = false;
                        
                        noDataTimes++;

                        if ( noDataTimes > 60 )
                        {
                            log.info(" no message in 60 second! topic="+job.topic);
                            noDataTimes = 0;
                        }

                        Tool.SleepAWhileInMS(200);
                        continue;
                    }               

                    hasRemainingData = true;
                    
                    noDataTimes = 0;

                    totalNumber++;
                    currentNo++;

                    int j = 0;

                    while(true)
                    {
                        j++;
                        errorInfo = messageProcessor.processMessages(dataService,totalNumber,topic,message,processingInfoMap);

                        if ( errorInfo.isEmpty() )
                            break;

                        Tool.SleepAWhile(1, 0);

                        dsConn.reConnect();
                        dataService = dsConn.getClient();

                        log.info(" messageProcessor.processMessages failed! topic="+job.topic);
                        processingInfoMap.put("processingInfo", "processs message failed! "+errorInfo);

                        try
                        {
                            dataService.updateKafkaConnectionLatestStatus(job.getOrganizationId(), job.getRepositoryId(), job.getDatasourceId(), computingNode.getId(),processingInfoMap.get("lastProcessedMessage"),processingInfoMap.get("processingInfo"));
                        }
                        catch(Exception e)
                        {
                            log.error("333333333333333333333 updateKafkaConnectionLatestStatus() failed! e="+e);

                            dsConn.reConnect();
                            dataService = dsConn.getClient();
                        }
                    }

                    if ( totalNumber%5000 == 0 )
                    {
                        try
                        {
                            //log.info(" update status when finish batch! topic="+job.topic);
                            dataService.updateKafkaConnectionLatestStatus(job.getOrganizationId(), job.getRepositoryId(), job.getDatasourceId(), computingNode.getId(),processingInfoMap.get("lastProcessedMessage"),processingInfoMap.get("processingInfo"));
                        }
                        catch(Exception e)
                        {
                            log.error("4444444444444444 updateKafkaConnectionLatestStatus() failed! e="+e);

                            dsConn.reConnect();
                            dataService = dsConn.getClient();

                            Tool.SleepAWhileInMS(500);
                        }
                    }
                }
            }
            catch(Throwable e)
            {
                log.error(" process message failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                Tool.SleepAWhile(1, 0);
            }
            finally
            {                   
                if ( dsConn!=null )
                    dsConn.close();
            }
        }
    }
    
    class SQLHandler implements Runnable
    {
        volatile String[] childThreadStatus = new String[NUMBER_OF_WRITE_DB_THREAD];
        private Thread[] childThreadList = new Thread[NUMBER_OF_WRITE_DB_THREAD];
        private DBWriter[] childDBWriterList = new DBWriter[NUMBER_OF_WRITE_DB_THREAD];
        private CreateKafkaConnectionJob job;
        private DataService.Client dataService = null;
        private DataServiceConnector dsConn = new DataServiceConnector();
        private String dataServiceIps;
        private Map<String,String> insertSqlCache;
        private Map<String,String> updateSqlCache;
        private List<String> insertSqlDataList= new ArrayList<>();
        private List<String> updateSqlDataList = new ArrayList<>();
        private int currentPosition;
        private volatile boolean hasRemainingData = true;
                
        public SQLHandler(String dataServiceIps,CreateKafkaConnectionJob job) 
        {
            this.job = job;
            this.dataServiceIps = dataServiceIps;
        }
        
        public boolean hasRemainingData()
        {
            return hasRemainingData;
        }
        
        @Override
        public void run()
        {
            long totalNumber = 0;
            Date startTime = new Date();
            String message;
            DatasourceConnection repositoryDatasourceConnection;
  
            try
            {
                while(true)
                {
                    log.info("1111111111111111create SQLHandler! topic="+job.topic);
                 
                    try
                    {
                        dataService = dsConn.getClient(dataServiceIps);
                        processingInfoMap.clear();

                        log.info(" dataService.getRepositorySQLdbDatasourceConnection() topic="+job.topic);
                        repositoryDatasourceConnection = (DatasourceConnection)Tool.deserializeObject(dataService.getRepositorySQLdbDatasourceConnection(job.getOrganizationId(),job.getRepositoryId()).array()); 
                        break;
                    }
                    catch(Exception e)
                    {
                        log.error(" dataService.getRepositorySQLdbDatasourceConnection() failed! topic="+job.topic+" e="+e);
                        dsConn.reConnect();
                        dataService = dsConn.getClient();

                        Tool.SleepAWhileInMS(500);
                    }
                }

                for(int i=0;i<NUMBER_OF_WRITE_DB_THREAD;i++)
                {
                    childThreadStatus[i] = "waiting";

                    if ( childThreadList[i] == null )
                    {
                        DBWriter dbWriter = new DBWriter(insertSqlDataList,updateSqlDataList,dsConn,i,childThreadStatus,repositoryDatasourceConnection);
                        Thread thread = new Thread(dbWriter);
                        thread.start();
                        childThreadList[i] = thread;
                        childDBWriterList[i]= dbWriter;
                        log.info(" create DBWriter thread! i="+i+" topic="+job.topic);
                    }
                }

                while(true)
                {
                    log.info("read from sqlCacheReadyQueue ...  topic="+job.topic);

                    message = sqlCacheReadyQueue.take();  // will block when no new message
                     
                    hasRemainingData = true;
                    
                    log.debug("222222222222 get sqlCacheReadyQueue message ="+message);
                    
                    currentPosition = Integer.parseInt(message);
                    insertSqlCache = insertSqlCacheList.get(currentPosition);
            
                    insertSqlDataList.clear();                            
                    for(Map.Entry<String,String> map : insertSqlCache.entrySet() )
                        insertSqlDataList.add(map.getValue());

                    if ( !insertSqlDataList.isEmpty() )
                    {
                        for(int i=0;i<NUMBER_OF_WRITE_DB_THREAD;i++)
                            childThreadStatus[i]="insert";

                        int retry = 0;

                        while(true)
                        {
                            retry ++;

                            log.debug(" check insert result! retry="+retry);

                            boolean allDone = true;

                            for(int i=0;i<NUMBER_OF_WRITE_DB_THREAD;i++)
                            {
                                log.debug(" no="+i+" status="+childThreadStatus[i]+" thread="+childThreadList[i]);

                                if ( !childThreadStatus[i].equals("done") )
                                    allDone = false;
                            }

                            if ( allDone )
                                break;

                            Tool.SleepAWhileInMS(1000);

                            //if ( retry > 200 )
                            //    throw new Exception("write to db failed!");
                        }
                    }
              
                    updateSqlCache = updateSqlCacheList.get(currentPosition);
                
                    updateSqlDataList.clear();
                    for(Map.Entry<String,String> map : updateSqlCache.entrySet() )
                        updateSqlDataList.add(map.getValue());

                    if ( !updateSqlDataList.isEmpty() )
                    {         
                        for(int i=0;i<NUMBER_OF_WRITE_DB_THREAD;i++)
                            childThreadStatus[i]= "update";
                        
                        int retry = 0;

                        while(true)
                        {
                            retry ++;

                            log.debug(" check update result! retry="+retry);

                            boolean allDone = true;

                            for(int i=0;i<NUMBER_OF_WRITE_DB_THREAD;i++)
                            {
                                log.debug(" no="+i+" status="+childThreadStatus[i]+" thread="+childThreadList[i]);

                                if ( !childThreadStatus[i].equals("done") )
                                    allDone = false;
                            }

                            if ( allDone )
                            {
                                for(int i=0;i<NUMBER_OF_WRITE_DB_THREAD;i++)
                                    childThreadStatus[i]="waiting";

                                break;
                            }

                            Tool.SleepAWhileInMS(1000);

                            //if ( retry > 200 )
                            //    throw new Exception("write to db failed!");
                        }
                    }

                    hasRemainingData = false;
                    
                    log.info("finish write to db! topic="+job.topic);
                }
            }
            catch(Throwable e)
            {
                log.error(" process message failed! e="+e+" stacktrace="+ExceptionUtils.getStackTrace(e));
                Tool.SleepAWhile(1, 0);
            }
            finally
            {                   
                if ( dsConn!=null )
                    dsConn.close();
            }
        }
    }
    
    class DBWriter implements Runnable 
    {       
        private int no;
        private List<String> insertSqlDataList;
        private List<String> updateSqlDataList;
        private List<String> sqlDataList;
        private String[] childThreadStatus;
        private Connection dbConn;
        private DatasourceConnection repositoryDatasourceConnection;
        private DataServiceConnector dsConn;
        
        public DBWriter(List<String> insertSqlDataList,List<String> updateSqlDataList,DataServiceConnector dsConn,int no, String[] childThreadStatus,DatasourceConnection repositoryDatasourceConnection) {
            this.no = no;
            this.childThreadStatus = childThreadStatus;
            this.repositoryDatasourceConnection = repositoryDatasourceConnection;
            this.dsConn = dsConn;
            this.insertSqlDataList = insertSqlDataList;
            this.updateSqlDataList = updateSqlDataList;
        }

        @Override
        public void run()
        {
            Statement stmt = null;

            Date startTime;          
            String sqlStr = null;
            String sql = null;
            String dataobjectTypeIdStr;
            boolean batchOperationFailed;
            boolean finishBatch;          
            int k = 0;
            int n = 0;
            boolean reachEnd;
            int dataobjectTypeId = 0;
                                                   
            while(true)
            {                
                log.debug("DBWriter check, thread=["+no+"[ statu=["+childThreadStatus[no]+"]" );
 
                if ( childThreadStatus[no].equals("waiting") || childThreadStatus[no].equals("done") )
                {
                    Tool.SleepAWhileInMS(300);
                    continue;
                }
                
                if ( childThreadStatus[no].equals("insert") )
                    sqlDataList = insertSqlDataList;
                else
                    sqlDataList = updateSqlDataList;
                
                if ( sqlDataList == null )
                {
                    log.info(" sqlCacheList == null !!!");
                    Tool.SleepAWhileInMS(300);
                    continue;
                }
                
                log.debug("start to "+childThreadStatus[no]+" sqlCacheList size="+sqlDataList.size() );
                 
                try
                {
                    int sizePerThread = KAFKA_MESSAGE_COMMIT_SIZE/NUMBER_OF_WRITE_DB_THREAD;
                    int start = no * sizePerThread;
                                  
                    if ( sqlDataList.isEmpty() || sqlDataList.size() < start )
                    {
                        log.info(" sqlcacheList size = "+sqlDataList.size()+" is smaller than start="+start+" sizePerThread="+sizePerThread);
                        childThreadStatus[no] ="done";
                        continue;
                    }
                    
                    n++;
                    
                    if ( n%10000 == 0 )
                    {
                        log.info(" reconnect db! n="+n);
                        JDBCUtil.close(dbConn);   
                        dbConn = null;
                    }
                                        
                    if ( dbConn == null )
                    {
                        dbConn = JDBCUtil.getJdbcConnection(repositoryDatasourceConnection); 
                        log.info("create new dbConn. thread="+no+" dbConn="+dbConn);
                        dbConn.setAutoCommit(false);
                    }
                    
                    batchOperationFailed = false;
                    startTime = new Date();

                    stmt = dbConn.createStatement();
                    stmt.setQueryTimeout(CommonKeys.JDBC_QUERY_TIMEOUT*2); // 6000 seconds 100分钟

                    int retry = 0;
                    
                    while(true)
                    {
                        retry++;
                        log.info("retry="+retry+", start to write to db, sqlCacheList size="+sqlDataList.size()+" batchOperationFailed="+batchOperationFailed );
                 
                        stmt.clearBatch();
                        reachEnd = false;
                                       
                        k = 0;
                        
                        for(int i=start;i<start+sizePerThread;i++)
                        {
                            //log.info("thread="+no+" position ="+i+" total size="+sqlDataList.size()+" start="+start);

                            int retry1 = 0;
                            
                            while(true)
                            {
                                retry1++;
                                
                                try
                                {
                                    if ( i >= sqlDataList.size() )
                                    {
                                        log.error(" reach end!");
                                        reachEnd = true;
                                        break;
                                    }

                                    sqlStr = sqlDataList.get(i);

                                    sql = sqlStr.substring(sqlStr.indexOf(" ")+1);
                                    
                                    dataobjectTypeIdStr = sqlStr.substring(0,sqlStr.indexOf(" "));
                                    dataobjectTypeId = Integer.parseInt(dataobjectTypeIdStr);
                             
                                    //log.info(" execute sql="+sql);
                                    
                                    if ( batchOperationFailed == false )
                                    {
                                        stmt.addBatch(sql);
                                        k++;
                                        //log.info("222 add batch sql="+sql.substring(0,40));
                                    }
                                    else
                                    {
                                        stmt.executeUpdate(sql);
                                        k++;
                                        //log.info("333 executeUpdate sql="+sql.substring(0,40));
                                    }
                                    
                                    break;
                                }
                                catch(Exception e)
                                {
                                    String sqlError = "000000 execute sql failed! e="+e+" sql="+sql;
                           
                                    try{
                                        dbConn.commit();
                                    }
                                    catch(Exception ee){
                                    }

                                    if ( sql.startsWith("INSERT INTO") )
                                    {
                                        if ( sqlError.toLowerCase().contains("duplicate") || sqlError.toLowerCase().contains("unique constraint") ) // if duplicted, regard it is successful
                                        {
                                            log.debug("insert duplicated data, regard it is successful, break; sql="+sql.substring(0,40));
                                            break;
                                        }
                                    }
                                
                                    log.error(sqlError);

                                    if ( sqlError.toLowerCase().contains("column")  )
                                    {
                                        String tableName = Tool.getTableNameFromSQLStr(sql);
                                                                                
                                        DatabaseType databaseType = DatabaseType.GREENPLUM;
                                        
                                        List<String> addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType,tableName,"edf_id","VARCHAR(40)","");

                                        try
                                        {
                                            try
                                            {
                                                for(String addColumnSql : addColumnSqlList)
                                                {
                                                    log.info(" exceute add edf_id column sql ="+addColumnSql);
                                                    stmt.executeUpdate(addColumnSql);
                                                }
                                             }
                                            catch(Exception ee)
                                            {
                                                log.error(" add edf_id column failed! e="+ee);
                                            }
                                                                                        
                                            try{
                                                dbConn.commit();
                                            }
                                            catch(Exception ee){
                                            }
                                                                                 
                                            List<Map<String,Object>>  metadataDefinitions = (List<Map<String,Object>>)Tool.deserializeObject(dsConn.getClient().getDataobjectTypeMetadataDefinition(dataobjectTypeId,false,true).array());
                                            DataobjectType dataobjectType = (DataobjectType)Tool.deserializeObject(dsConn.getClient().getDataobjectType(dataobjectTypeId).array());
                                            
                                            for(Map<String,Object> definition : metadataDefinitions)
                                            {
                                                String metadataName = (String)definition.get(CommonKeys.METADATA_NODE_NAME);
                                                String description = (String)definition.get(CommonKeys.METADATA_NODE_DESCRIPTION);
                                                String dataType = (String)definition.get(CommonKeys.METADATA_NODE_DATA_TYPE);
                                                String lenStr = (String)definition.get(CommonKeys.METADATA_NODE_LENGTH);
                                                int len;

                                                if ( lenStr == null || lenStr.trim().isEmpty() )
                                                    len = 0;
                                                else
                                                    len = Integer.parseInt(lenStr);

                                                log.info(" tableNamename="+tableName+" metadataName="+metadataName);
                                                String newColumnName = metadataName.toLowerCase().replace(dataobjectType.getName().trim().toLowerCase()+"_", "");
                                                newColumnName = newColumnName.toUpperCase();

                                                log.info(" newColumnName="+newColumnName);

                                                String sqlType = MetadataDataType.findByValue(Integer.parseInt(dataType)).getJdbcType();

                                                if ( sqlType.contains("%d") )
                                                    sqlType = String.format(sqlType,len);

                                                addColumnSqlList = JDBCUtil.getAddColumnSqlList(databaseType,tableName,newColumnName,sqlType,description);

                                                try
                                                {
                                                    try{
                                                        dbConn.commit();
                                                    }
                                                    catch(Exception ee){
                                                    }
                                                       
                                                    for(String addColumnSql : addColumnSqlList)
                                                    {
                                                        log.info("111 exceute add column sql ="+addColumnSql);
                                                        stmt.executeUpdate(addColumnSql);
                                                    }
                                                }
                                                catch(Exception ee)
                                                {
                                                    log.error(" add column failed! e="+ee);
                                                }
                                            }
                                        }
                                        catch(Exception ee)
                                        {
                                            log.error(" add column failed! e="+ee);
                                        }      
                                        
                                        try{
                                            dbConn.commit();
                                        }
                                        catch(Exception eee){
                                        }
                                    }                                    
                                                                        
                                    if ( retry1 >= 2 )
                                    {
                                        log.error("11111111111111 throw exception !!!");
                                        throw e;
                                    }
                                }
                            }
                            
                            if ( reachEnd == true)
                                break;
                        }
                     
                        finishBatch = false;
   
                        if ( batchOperationFailed == false )
                        {
                            try
                            {
                                int[] ret = stmt.executeBatch();
                                log.info(String.format("finish execute batch!!! took %d ms, size=%d",(new Date().getTime() - startTime.getTime()),k ) );

                                finishBatch = true;
                            }
                            catch(Exception e)
                            {
                                String errorInfo = "7777777777777777 execute batch failed! ";
                                log.error(errorInfo);
 
                                try{
                                    dbConn.commit();
                                }
                                catch(Throwable ee){
                                }
                                
                              /*  try
                                {
                                    dsConn.getClient().updateKafkaConnectionLatestStatus(job.getOrganizationId(), job.getRepositoryId(), job.getDatasourceId(), computingNode.getId(),null,errorInfo);
                                }
                                catch(Exception ee)
                                {
                                     log.error(" updateKafkaConnectionLatestStatus failed when execute batch failed! ee="+ee);   
                                }*/

                                batchOperationFailed = true;
                            }
                        }  
                        else
                            break;

                        if ( batchOperationFailed == false && finishBatch )
                            break; 
                    }
                                        
                    stmt.close();
                    
                    //log.info(String.format("finish insert/update !!! took %d ms, size=%d",(new Date().getTime() - startTime.getTime()),k ) );  
                    
                    startTime = new Date();
                            
                    dbConn.commit();
                    
                    log.info(String.format("finish commit db!!! took %d ms, size=%d",(new Date().getTime() - startTime.getTime()),k ) );  
                                                        
                    childThreadStatus[no] ="done";
                }
                catch(Exception e)
                {
                    String errorInfo = ("22222222222222 dbWriter.run() failed! e="+e+",sql=["+sql+"], stacktrace="+ExceptionUtils.getStackTrace(e));
                    log.error(errorInfo);
                    
                    JDBCUtil.close(stmt);
                    JDBCUtil.close(dbConn);                    

                    try
                    {
                        dsConn.getClient().updateKafkaConnectionLatestStatus(job.getOrganizationId(), job.getRepositoryId(), job.getDatasourceId(), computingNode.getId(),null,errorInfo);
                    }
                    catch(Exception ee)
                    {
                         log.error(" updateKafkaConnectionLatestStatus failed when updating db failed! ee="+ee);   
                         dsConn.reConnect();
                    }
                        
                    Tool.SleepAWhileInMS(5000);
                    
                    dbConn = null;
                }
            }
        }    
    }
}
