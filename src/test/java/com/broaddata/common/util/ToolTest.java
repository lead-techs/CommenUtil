/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.broaddata.common.util;

import com.broaddata.common.model.enumeration.ComputingNodeService;
import com.broaddata.common.util.Tool;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.dom4j.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author fxy1949
 */
public class ToolTest {
    
    public ToolTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
     
    @Ignore
    @Test
    public void test1()
    {
        String tmpdir = Tool.getSystemTmpdir();
        
        System.out.println(tmpdir + ":" + tmpdir);
        
    }
    
    @Ignore
    @Test
    public void testExtractValueByRe()
    {
        String content = "D:\\A_testdata\\case_video\\dianti\\aaaa\\channel13_2017121309373471404617_20181203121015.mp4";
        String pattern = "channel.*?_";
        
        String val = Tool.extractValueByRE(content, pattern, 1);
        
        System.out.println(" val="+val);
        
        String pattern1 = "[0-9]{14}";
        
        val = Tool.extractValueByRE(content, pattern1, 1);
        
        System.out.println(" val="+val);
    }
    
    
    @Ignore
    @Test
    public void querySS() throws Exception
    {
        String targetIp = "192.168.0.17:32154";
        String indexName = "face_repository_9";
        
        String targetURL = String.format("http://%s/vector/similarity",targetIp);
        String requestData = String.format("{\"name\":\"%s\",\"dimension\":512}",indexName);
                            
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("index",indexName);
        jsonObject.put("k",10);
        
        JSONArray array = new JSONArray();
        jsonObject.put("vectors", array);
            
        JSONObject columnObject = new JSONObject();

        columnObject.put("name", String.format("%d",1));
        columnObject.put("embeddings", "0.015757505,-0.049735878,-0.10008878,0.06973403,0.01600966,0.01053793,0.053540528,0.046102427,-0.047098033,-0.046477307,0.021531258,0.008066314,0.04625308,0.009209465,0.013455346,0.00043373782,0.036589004,0.027509345,0.03853918,-0.03105057,0.042608827,0.011819026,0.068464175,-0.01907815,0.019966614,0.023678059,0.09444209,-0.044450063,0.0122807855,-0.008027942,0.054056358,0.0049776565,-0.057337485,0.0071960357,-0.023986472,-0.0011854187,-0.040491864,-0.0030374734,-0.08290627,-0.006171548,-0.0370827,-0.04540123,0.02329097,0.01480186,0.046082553,-0.003467251,0.04625477,0.07942284,-0.074533805,0.0012616188,0.057044942,0.04133137,0.054958735,-0.057363544,-0.07191198,-0.013255278,-0.008893049,0.051639065,0.004042527,0.052228477,0.0034709452,0.021646563,0.02931526,-0.03919192,-0.022898177,0.019545758,-0.015420172,-0.0250126,0.006371858,-0.025071988,-0.0036757153,0.018516125,0.025820618,-0.019935934,0.02980724,0.007079078,0.060773596,-0.0936053,-0.04469416,0.006830737,-0.018978992,0.04637196,0.013911105,-0.0036585627,-0.052540537,-0.024787763,0.07027398,0.01748424,0.022415733,-0.008234123,-0.051082443,0.025413869,-0.010195817,-0.0064022425,0.08405748,-0.030566696,-0.052360605,-0.047603667,-0.1059119,0.017854875,0.05307571,-0.041447293,0.053648397,0.01971194,-0.04220099,0.042906985,0.0081718005,0.011607371,-0.00301132,-0.011618991,0.022015836,-0.0043939217,-0.029393824,-0.050696593,0.015973993,-0.1008117,-0.03431971,0.013388579,-0.039747104,0.061850272,0.01996749,0.023739377,0.013493597,-0.028920334,-0.12943679,-0.052904025,-0.065948196,0.07046337,0.10737981,-0.032723743,0.05367505,-0.033158407,0.018320462,-0.011500759,-0.0563261,0.08040679,-0.004778763,0.025915867,0.025515003,0.058255352,0.03835827,0.04142554,-0.003955036,-0.028714694,0.049674816,0.017615002,0.033889167,-0.03537103,0.0844435,0.03136733,0.0047563445,0.020668091,-0.019702164,0.020484865,0.0032152126,-0.10919147,0.015670776,-0.013769301,0.030143859,-0.013247578,0.014720294,-0.010719621,0.0026958552,0.062019233,0.09155134,-0.0118677635,-0.04825238,-0.0051200124,-0.067333974,-0.05766219,0.057634994,0.04811169,0.008414218,-0.018447908,0.0010909339,0.013470071,-0.052815128,-0.01776009,-0.007349872,0.0947702,-0.07740198,0.047691863,-0.045062322,-0.04226071,-0.027451832,0.020377036,0.0135846445,-0.006415342,-0.04345094,-0.009197216,0.02140482,-0.07667411,0.07759383,-0.020088285,0.033659376,0.0034192894,-0.008431646,0.025045922,-0.06924645,0.027313694,0.05645478,-0.015075884,-0.03132873,0.0118111,0.026197787,-0.0008319849,0.044133607,-0.066783905,-0.073255055,-0.027559089,0.040759113,-0.00091006904,-0.03419202,0.06298591,-0.052165315,0.056029867,-0.014486604,0.0013515765,0.07873854,-0.015292485,-0.0025450925,-0.083278954,-0.021772556,0.00036515438,0.05760923,0.03968607,-0.023646044,-0.0069356975,-0.032136176,-0.007684632,0.018085964,0.038161434,0.03649476,-0.01697646,-0.03221357,0.03570967,-0.053186186,-0.026290782,0.023339694,-0.09334718,-0.038220108,-0.08199124,0.047600567,-0.06770819,0.13415727,0.0859295,-0.042051733,0.033611916,0.051382218,0.055397622,-0.08218285,0.013997442,-0.058432408,-0.015957033,-0.0027187981,-0.056500208,0.045950823,-0.016891332,0.03330865,-0.03185925,-0.013822524,-0.02869194,0.008407319,-0.014259597,-0.015360006,-0.044762433,-0.003937209,-0.066025205,0.028558403,0.034343608,-0.003582524,0.028715013,0.0035570597,-0.006311489,-0.0046510943,-0.032679003,-0.003664078,-0.0056587984,-0.052202247,0.013253921,0.0537212,-0.07404105,0.007496218,-0.1253822,-0.033713154,-0.015130739,-0.03145202,-0.04764793,-0.075855464,-0.0024402095,0.073649555,-0.030507227,0.0034764328,-0.06416528,-0.065346695,-0.0056823194,-0.025159148,-0.0031360781,0.045244247,-0.0024452244,0.083422594,-0.016140938,0.04527314,-0.064748816,0.043762602,-0.010483083,-0.06833378,0.038579363,-0.025284698,0.071459346,-0.008489032,0.0188977,-0.012855314,-0.008503177,-0.008041055,0.079337634,0.0141150495,-0.03109778,0.0569805,-0.04326172,0.06648538,-0.055677295,-0.04084396,-0.034815103,0.065992065,0.035414647,0.042217184,0.053987056,0.068865865,0.022585448,0.038154665,0.0865926,-0.01756629,0.0820587,0.012345572,-0.037607227,0.018217549,-0.009218542,-0.019998914,-0.036890615,0.04416152,-0.0030325942,0.03371427,0.032742854,0.010661164,0.054729357,0.01775078,0.06029766,-0.00381894,-0.041533962,-0.0038555975,-0.0641971,0.026895072,-0.040465694,0.0150048435,0.04998849,0.03624158,0.015080781,-0.0068971757,-0.008672907,0.047314044,0.016891254,-0.05001584,-0.04134753,-0.037826058,-0.01001154,0.011424355,-0.057567496,-0.010123232,-0.10470011,-0.037710764,0.041107435,-0.0010279856,0.020217035,-0.017450867,0.05557446,-0.04138512,0.09146416,-0.052457135,-0.04124882,-0.07641961,0.034404088,-0.031152243,-0.019091647,0.0015770302,-0.009827592,-0.017628632,-0.035542425,-0.062442854,-0.06420433,0.009450643,-0.010336794,-0.036378056,-0.055320613,0.03293303,-0.024755074,-0.022148568,0.09593883,0.036925435,0.026830489,-0.020128978,0.024982061,0.0048093456,0.041403025,-0.058769446,0.041087728,0.019508718,-0.03994468,0.009756656,-0.010487829,0.025430257,0.016481312,0.007117532,0.033810657,0.0034564247,-0.048043925,0.024060598,0.047487017,0.0031214664,-0.021853663,0.041415047,-0.017715449,-0.024504695,0.01991082,-0.021856705,-0.023553314,0.023939664,0.08483353,0.07283847,-0.04528098,-0.08201364,-0.033255443,-0.01940172,-0.056664772,-0.05974288,-0.019742059,-0.010456679,0.008982563,0.03057297,-0.022463199,-0.046525527,-0.02598809,0.016557762,0.071238026,0.013921009,-0.015460144,-0.03878945,0.04069621,0.021796029,0.015629739,0.026288493,0.05522023,-0.049393523,-0.023470722,-0.011371681,-0.015779339,-0.06792961,0.05100245,0.05629412,0.09505925,-0.034510233,0.020508286,-0.012138928,0.05117464,0.030824384,-0.0104826465,-0.03044414,0.052735932,0.04445056,0.023123642,0.05244894,0.058404725,0.08922868,-0.0009579875,-0.10639251,0.011187041,0.0041420534,0.042470466,0.018412495,-0.06551529,-0.083654724,-0.046794027,0.06104968,0.019391987,-0.05394093,0.044504546,-0.09165067,0.07287827,-0.00960629,-0.07235275,0.021472745,-0.012704957,-0.023844844,-0.033435833,0.05968966,-0.0018159068,0.04964122,0.0018594408,0.09770774,0.0013396334,0.011932833,-0.03048203,-0.013782597,-0.022056116,-0.011761597,0.06329791,0.082033336,0.0022197294,-0.1182148,0.051084366,0.029479014,-0.07646478");

        array.put(0, columnObject);
        
        columnObject = new JSONObject();

        columnObject.put("name", String.format("%d",2));
        columnObject.put("embeddings", "0.09116302,-0.045291033,-0.07122039,-0.011189504,-0.02944091,0.004781485,-0.07634218,0.08311634,0.022165896,-0.036020514,0.033500087,-0.011751484,-0.027646434,-0.06630611,-0.005222779,0.053546615,0.037594255,-0.006028352,0.008073066,0.027054599,0.0027568645,0.02665652,0.047757756,-0.070726514,-0.039166633,-0.007850048,0.029101027,0.050108943,0.00078733964,0.042026784,-0.02352337,0.034125797,-0.017574295,0.042374596,0.02779578,0.023519823,0.013647027,0.00875455,-0.009346524,-0.0452267,0.04452911,-0.0016768973,0.015832582,-0.010880687,-0.0069947788,-0.020608826,-0.022918135,-0.027992226,-0.04502407,0.006857389,-0.01146726,-0.028604979,0.023946032,-0.012661033,-0.061131995,0.09139236,-0.041067258,0.08568538,0.10292235,0.05084565,-0.0014194178,0.025080627,-0.05907922,0.05867579,0.010435315,0.052921247,0.04534367,-0.014368035,0.035778437,-0.039102383,-0.08021869,-0.03349894,0.06514528,-0.002825395,0.015674133,-0.0666786,-0.09295323,-0.02369115,-0.013381237,-0.016119856,0.03459163,-0.0111279,-0.013228097,0.020682445,0.03869942,-0.01821908,-0.013280738,0.0667559,-0.036104806,0.043521147,-0.018271975,-0.021095697,0.062737316,-0.06115546,-0.07853347,-0.009478492,0.04781942,0.06522347,-0.04960933,-0.046016503,0.007031458,0.030970074,-0.021149311,0.08452477,0.05200229,-0.050951295,0.0840456,-0.061853494,0.030656818,0.018007597,-0.021937784,0.00078890554,-0.016265295,-0.068831444,0.014257738,-0.030745868,0.0148893185,0.003356483,0.04178666,0.04638164,-0.03788453,-0.01668951,-0.053180758,-0.01420645,-0.02003545,-0.010665028,0.097361885,-0.01806084,-0.04492919,-0.019599717,-0.09716198,-0.00017902986,0.0004975173,-0.053574365,-0.024342705,0.032496445,-0.020218147,0.022363402,0.060800016,0.07999659,0.05880884,-0.042319715,-0.025490474,0.044774223,0.004448928,-0.09837764,-0.060688756,0.029907007,0.04653523,0.06497985,0.093344614,0.016122948,0.030546218,-0.008092253,6.28948e-05,-0.011712659,-0.044102762,-0.030373141,-0.02814915,-0.023425866,0.05327244,-0.025216091,-0.0075168055,-0.008191454,0.037790313,-0.033988293,-0.010882166,-0.04581695,0.017037306,0.051525444,0.0075414013,0.026793083,-0.038563885,-0.0054879743,0.066887505,0.031148005,-0.0010629289,0.030404357,0.0039861165,-0.019349074,-0.040625088,0.053528775,-0.051037923,0.0007926828,0.014412186,0.04628807,0.013217876,0.06565152,0.0031693613,0.044450156,0.10898565,-0.009782728,0.005213895,-0.0637648,0.11037718,0.032769583,-0.01243878,0.09227329,-0.00867006,-0.008032657,0.05428349,0.04894401,0.03435346,-0.034548044,0.04313675,0.0709068,0.0034641929,0.011074313,0.013306385,0.07184431,0.0036287543,0.011305795,0.008874038,-0.004399907,-0.057574082,0.030746866,-0.03421192,0.008900049,-0.03179388,0.052767713,-0.024273444,0.019198254,-0.04308704,-0.005528563,0.023114592,0.03187463,-0.023629824,0.018619215,-0.021877628,0.06639074,0.05007692,-0.08915638,-0.015619489,-0.07975616,0.0064538363,-0.0050723334,0.02761487,-0.027167683,-0.047852065,-0.02440227,0.038899723,0.0437173,0.08326788,-0.056126546,0.016776184,0.09015972,-0.07736559,0.040829703,0.01749441,0.012777865,-0.053476125,0.032950293,-0.014369844,-0.025748568,-0.0059234323,-0.024791524,0.00062961876,-0.0039600935,0.07584639,0.008548243,-0.02912204,-0.07590893,0.008755155,-0.018600024,0.025153881,-0.06924825,0.019003766,0.050703682,-0.027113184,-0.07580748,-0.0134769175,0.05645289,0.04260497,0.03242862,-0.017638471,0.008633972,-0.05033202,-0.062000036,-0.043424286,-0.008333816,-0.051114853,0.083395585,0.022531342,-0.09025694,-0.008057637,-0.041085154,0.005864298,0.060721386,0.001506103,0.0061648,-0.003231283,0.017597983,-0.036200713,0.0384655,-0.0046171937,-0.039271113,0.057301972,0.024626484,0.004802089,-0.024018671,0.026502918,0.007788168,-0.044944603,0.023931002,-0.029888023,0.02888444,0.03990737,-0.048456322,0.032149434,0.003978302,-0.001974915,-0.00057721604,-0.06317861,-0.079307824,0.028327268,-0.005296088,-0.010086956,0.03416365,0.12291289,-0.10688262,0.00636366,-0.1244295,-0.032321375,0.020102816,-0.018860076,0.012140993,0.004105439,0.026573617,0.008730196,0.0072811237,0.050822623,-0.011755399,-0.015146513,-0.035338655,0.08616415,0.09318872,0.0021895065,-0.052074943,-0.0001283417,-0.06518969,0.09266287,0.017039202,-0.021054571,-0.017198285,-0.058152314,-0.029465733,0.028589062,0.037930403,-0.023811469,0.08267677,0.037453633,-0.037114054,0.009315483,0.057660647,-0.054437775,-0.044152524,0.038539335,0.022675024,-0.05785074,0.0037583702,-0.04913154,-0.006771615,-0.022889147,-0.06635195,-0.022553079,0.016250793,0.033053737,-0.06356613,-0.017606013,-0.027123751,-0.094316185,0.016121585,-0.13270555,0.0069804364,0.013323288,0.032984227,0.020051232,0.02723441,-0.0071751503,0.047287524,-0.071415894,0.013743341,-0.0013018223,0.03991198,0.11006005,-0.0537918,0.06846243,0.047297124,-0.010764268,-0.050684642,0.04249221,0.045209926,-0.04999706,-0.0754088,0.026745632,-0.000190937,0.07370867,0.029390674,0.012916453,-0.035794348,0.060984466,-0.016941037,0.0377996,-0.006498771,0.0050047613,-0.016162952,-0.063473016,-0.093776494,0.038945027,-0.00024803213,0.13088922,-0.005642243,0.015137567,0.0059435656,-0.04710387,-0.053108532,0.0636079,0.047564905,-0.06336464,-0.0027652597,0.08113182,0.038131893,0.047838554,0.03690972,-0.027168402,-0.022723947,0.018312918,-0.02810968,-0.045639627,-0.08441653,-0.023844313,-0.005309446,-0.011719179,-0.06697829,0.015571734,0.057157945,0.07125019,0.0064060134,-0.018960869,-0.026941614,-0.05568484,0.035271097,0.039124187,0.032210976,0.014785203,0.0016625352,-0.008890027,0.0322863,-0.012272126,0.0048698224,0.079594105,-0.021046026,0.054353345,0.07500379,-0.0027829837,-0.03199313,-0.007941179,0.0014115694,0.04049802,0.04108228,0.017857155,0.0202618,0.026674177,-0.03464412,-0.0010317315,-0.0067250114,0.009551392,-0.086422645,0.0280308,0.020539496,0.031332683,-0.005605161,0.067879885,-0.069103375,-0.011986653,0.047838796,0.0067098746,0.012232663,0.030584209,-0.04300184,0.010096757,0.0066705816,0.025029235,0.008659483,-0.0014168349,-0.04993689,-0.056535244,0.04752675,-0.023237184,-0.08837024,0.015748348,-0.047039546,-0.029621897,0.029357221,0.03391735,0.05174041,0.031793702,-0.03683233,0.034677945,-0.0628984,-0.05213681,0.069505736,-0.097076505,0.014567232,-0.01176408,-0.08249283,-0.0013965343,-0.06386432,-0.03582006,0.05535779,0.024036776,-0.0074088755");
        array.put(1, columnObject);
                                    
        String responseStr = Tool.invokeRestfulService(targetURL,null,jsonObject.toString(),1);
        System.out.println("insert responseStr="+responseStr);
    }
    
     @Ignore
    @Test
    public void testURL()throws Exception
    {
 
        String targetIp = "192.168.0.17:32154";
        String indexName = "face_repository_8";
        
        String targetURL = String.format("http://%s/index/create",targetIp);
        String requestData = String.format("{\"name\":\"%s\",\"dimension\":512}",indexName);
                            
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("name",indexName);
        jsonObject.put("dimension",512);
                        
        String responseStr = Tool.invokeRestfulService(targetURL,null,jsonObject.toString(),1);
        System.out.println("insert responseStr="+responseStr);
    }
    
   @Ignore
    @Test
    public void testSSHCall() throws Exception
    {
       String host = "192.168.0.17";
       String user = "root";
       String password = "root123";
       String command = "ffmpeg -y -ss 0 -t 10 -i /data/share/f7617b45-04e8-4bc7-86b8-b7ab1f554705/0_c01_20180730140801.mp4 -c:a aac -strict experimental -b:a 98k /data/share/f7617b45-04e8-4bc7-86b8-b7ab1f554705/new_0_c01_20180730140801.mp4 && touch /data/share/f7617b45-04e8-4bc7-86b8-b7ab1f554705/new_0_c01_20180730140801.mp4.done \n";
       
       Tool.executeSSHCommandChannelShell(host,22,user,password,command,3,"");       
    }
     
    @Ignore
    @Test
    public void testRegularExpression()
    {
        boolean matched;
        
        String regularExpression = "R33048323434";
        String value = "R33048323434";
        
        //String regularExpression = "^[A-Za-z0-9]+$";
        //String value = "azbc1233BBB";
        
        Pattern pattern  = Pattern.compile(regularExpression);
        matched = pattern.matcher(value).matches();
                     
        regularExpression = "^\\d{15}|\\d{18}$";
        value = "123456789012345123";
        
        pattern  = Pattern.compile(regularExpression);
        matched = pattern.matcher(value).matches();
    }
    
    @Ignore
    @Test
    public void test2222()
    {                 
        Map<String, String> map = new TreeMap<String, String>();     
        
        double factor = 3.1;
        String value = "111111";
        String key = String.format("%08.2f-%s",100-factor,value);
               
        map.put(key,value);
        
        factor = 3.1;
        value = "22222";
        key = String.format("%08.2f-%s",100-factor,value);

        map.put(key,value);
        
        factor = 6.1;
        value = "222222";
        key = String.format("%08.2f-%s",100-factor,value);
        map.put(key,value);

        Set<String> keySet = map.keySet();
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
            String key1 = iter.next();
            System.out.println(key1 + ":" + map.get(key1));
        }
    }
    
    @Ignore
    @Test
    public void test1111()
    {        
        String line = "1111,,,,,3,,,,N";
        String[] vals = line.split("[,]");      
        
        String aaa = "3333:333";
        
        String bbb = aaa.replace(":", ",");
        
        bbb.replace(",", "&");
        
        String ccc = aaa;
    }
    
    
    @Ignore
    @Test
    public void test222()
    {
        String str = "{call xxxx(?BUSINESS_DATE_yyyyMMdd)}";
        String storeProcedureParameterStr = str.substring(str.indexOf("(")+1, str.indexOf(")"));
         
        
        boolean c = Tool.onlyContainsBlank(str);
        
        str = "123 ";
        c = Tool.onlyContainsBlank(str);
        
        str = "     ";
        c = Tool.onlyContainsBlank(str);
        
        String indexName = "error:index1;index2;";
        indexName = indexName.substring("error:".length());
        String[] indexs = indexName.split("\\;");

        for(String index : indexs )
        {
            System.out.println(" index="+index);

        }

            
       int k = 1200 / 900 +1;
        
       
       String filename = "2342134.1.7.1.txt";
       String pattern = "2342134.*txt";
        
        
      boolean a =  filename.matches(pattern);
      
      System.out.println(" a="+a);
          
    }
    
  @Ignore
    @Test
    public void test111()
    {
        //String timeStr = "2016-01-02 12:34:33";
     
        //String ret = Tool.getOralceDatetimeFormat(timeStr,"yyyy-MM-dd");
        
        //ret = Tool.getOralceDatetimeFormat(timeStr,"yyyy-MM-dd hh24:mi:ss");

       // String dateStr = "0";
         
        //Date date = new Date(Long.parseLong(dateStr));
        //String aaa = Tool.getYearMonthStr(date);
                                
        //String str = "字段_多名,几噶;哈;";
  
        //String ret = Tool.removeSpecificCharForNameAndDescription(str);
        //String sql = "select  count(*), sum(AC10AMT),avg(AC10AMT) from core_bdfmhqac where AC01AC15=:accno~账号";
        //String sql = "select * from jf_activity_lottery where p_id > ':city~城市', and val>0.0 and price > :price~价格 and tag=':aaa'";
        String sql = "select village_no,count(1),sum(order_amount) from cyz_simple_order where village_no=':village_no~商户' and create_time>=to_date(':currentDate~日期','yyyy-MM-dd hh:mi:ss')";;
        List<String> list = DataviewUtil.extraceSqlParameterStrs(sql);
        
        List<String> parameterList = new ArrayList<>();
        
        int i=1;
        
        for(String para : list)
        {
            i++;
            System.out.print(" para="+para);
            
            parameterList.add(String.format("%s:%s",para,String.valueOf(i)));
            
        }
        
        sql = DataviewUtil.replaceSqlWithParameters(sql, parameterList);
          
        String newSql = DataviewUtil.removeSqlParameterStr(sql);
        
        sql = "select * from jf_activity_lottery where p_id>'1'~城市~ and val>0.0";
                
        list = new ArrayList<>();
        list.add("城市:2");
        
        //String newSql = DataviewUtil.replaceSqlWithParameters(sql, list);
         
    }
  
    @Ignore
    @Test 
    public void testXXX() 
    {
        
        try
        {            
            String ret = Tool.changeTableColumnName("CORE_AAA","abcdef",true); // CORE_AAA, AAA_BBB AAA_1_BBB
            
            ret = Tool.changeTableColumnName("CORE_AAA","AAA_abcd3",true); // CORE_AAA, AAA_BBB AAA_1_BBB
            
            ret = Tool.changeTableColumnName("CORE_AAA","CORE_AAA_abcd3",true); // CORE_AAA, AAA_BBB AAA_1_BBB
            
            ret = Tool.changeTableColumnName("EBILLS_PA_ORG","PA_ORG_SHORTNAME",true); // CORE_AAA, AAA_BBB AAA_1_BBB
                
            
            String aaa = "234234.234234 . 2344 ";
            String bbb = Tool.removeKeyValueSpace(aaa);
            
            System.out.println("aaa = ["+aaa+"] bbb=["+bbb+"]");
            
            aaa = " 234234.234234 . 2344.";
            bbb = Tool.removeKeyValueSpace(aaa);
            System.out.println("aaa = ["+aaa+"] bbb=["+bbb+"]");
            
            aaa = ". 234234 .234234 . 2344.";
            bbb = Tool.removeKeyValueSpace(aaa);
            System.out.println("aaa = ["+aaa+"] bbb=["+bbb+"]");
            
            aaa = " .234234 .234234 . 2344.";
            bbb = Tool.removeKeyValueSpace(aaa);
            System.out.println("aaa = ["+aaa+"] bbb=["+bbb+"]");
            
            String format = "12,4";
            String val = Tool.getDefaultNullVal(format); // 12,2
            
            
            String data = "              ";
            format = "yyyyMMddHHmmss";
            
            data = data.trim();
            
            format = format.substring(0,data.length());
            
            Date date = Tool.convertDateStringToDateAccordingToPattern(data, format);
            
            date = Tool.convertDateStringToDateAccordingToPattern("              ", "yyyyMMddHHmmss");
        }
        catch(Exception e)
        {
            
        }
        
        
        
        //String[] a = new String[]{"111","222","333","444"};
        
 
        //String[] b = Tool.shuffleArray(a);
          
       // String str = "123123.0";
      //  str = Tool.normalizeNumber(str);
      //  System.out.print(str);
        Date date = new Date();
        
        Date date1 = Tool.changeToGmt(date );
        
        long l = 100000;
        double k = (double)l/(double)CommonKeys.DATAOBJECT_NUMBER_IN_CACHE_UP_LIMIT*100;
          int kk = (int)k;
        String str1 = "0000000016152.24";
        
        double d = Double.parseDouble(str1);
      
                
        BigDecimal bd = new BigDecimal(str1);  
        String vv = bd.toPlainString();
        
        boolean a = Tool.isNumber(str1);
        String str2 = Tool.normalizeNumber(str1);
        System.out.print(str2);
    }
@Ignore
    @Test 
    public void textyyy() throws Exception 
    {
        int serviceId = 4;
   
        long processedItemsInLastThreeMinutes = (Long)Tool.getServiceData("localhost", ComputingNodeService.findByValue(serviceId).getJmxPort(), CommonKeys.JMX_DOMAIN, CommonKeys.JMX_TYPE_EDF_NODE, "getPerformanceData",CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_COUNT,Calendar.MINUTE,-30000);
        long processedItemsTimeTakenInLastThreeMinutes = (Long)Tool.getServiceData("localhost", ComputingNodeService.findByValue(serviceId).getJmxPort(), CommonKeys.JMX_DOMAIN, CommonKeys.JMX_TYPE_EDF_NODE, "getPerformanceData",CommonKeys.PERFORMANCE_COUNTER_PRCESSED_ITEM_TIME_TAKEN,Calendar.MINUTE,-30000);
         
        long avgTime =  processedItemsTimeTakenInLastThreeMinutes/processedItemsInLastThreeMinutes;
       
        
        fail("333");
    }
    
    @Ignore
    @Test
    public void testxxx() {
           
            String filter = "( ( file_type.file_ext:xlsx OR file_type.file_ext:jar) AND ( xxx OR yyy) )";
            filter = Tool.removeFirstAndLastChar(filter.trim());
            
            String[] andStrs = filter.split("AND");
            
            for(String andStr : andStrs ) // each and
            {
                //selectedGroupCriteria=
                
                andStr = Tool.removeFirstAndLastChar(andStr.trim());
                String[] orStrs = andStr.split("OR");
                
                for(String orStr : orStrs) // each or
                {                   
                    String field = orStr.substring(0,orStr.indexOf(":"));
                    String value = orStr.substring(orStr.indexOf(":")+1);

                }
                
                //addCriterion();
            }
    }
    
    /**
     * Test of getContentType method, of class Tool.
     */
    @Ignore
    @Test
    public void testGetContentType() {
        System.out.println("getContentType");
        String mimeType = "application/x-shockwave-flash";
        String expResult = "video";
      //  String result = Tool.getContentType(mimeType).toString().toLowerCase();
     //   assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
      //  fail("The test case is a prototype.");
    }

    /**
     * Test of getStringFromByteBuffer method, of class Tool.
     */
    @Ignore
    @Test
    public void testGetStringFromByteBuffer() throws Exception {
        System.out.println("getStringFromByteBuffer");
        ByteBuffer byteBuffer = null;
        String encoding = "";
        String expResult = "";
        String result = Tool.getStringFromByteBuffer(byteBuffer, encoding);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of converterToPinyinAcronym method, of class Tool.
     */
       @Ignore
    @Test
    public void testConverterToPinyinAcronym() {
        System.out.println("converterToPinyinAcronym");
        String chineseStr = "";
        String expResult = "";
        String result = Tool.converterToPinyinAcronym(chineseStr);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of converterToPinyin method, of class Tool.
     */
       @Ignore
    @Test
    public void testConverterToPinyin() {
        System.out.println("converterToPinyin");
        String chineseStr = "";
        String expResult = "";
        String result = Tool.converterToPinyin(chineseStr);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of parseJson method, of class Tool.
     */
    @Ignore
    @Test
    public void testParseJson() throws Exception {
        System.out.println("parseJson");
        JSONObject json = null;
        Map<String, String> expResult = null;
        Map<String, String> result = Tool.parseJson(json);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of convertStringToDate method, of class Tool.
     */
       @Ignore
    @Test
    public void testConvertStringToDate() {
        System.out.println("convertStringToDate");
        String dateStr = "";
        Date expResult = null;
        Date result = Tool.convertStringToDate(dateStr);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of convertDateToString method, of class Tool.
     */
       @Ignore
    @Test
    public void testConvertDateToString() {
        System.out.println("convertDateToString");
        Date date = null;
//        String expResult = "";
        //String result = Tool.convertDateToString(date);
      //  assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of convertDateToStringForES method, of class Tool.
     */
@Ignore
    @Test
    public void testConvertDateToStringForES() {
        System.out.println("convertDateToStringForES");
        Date date = new Date();
        String expResult = "";
//        String result = Tool.convertDateToStringForES(date);

    }

    /**
     * Test of getMediaPlayer method, of class Tool.
     */
       @Ignore
    @Test
    public void testGetMediaPlayer() {
        System.out.println("getMediaPlayer");
        String mimeType = "";
        String expResult = "";
        String result = Tool.getMediaPlayer(mimeType);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getXmlDocument method, of class Tool.
     */
       @Ignore
    @Test
    public void testGetXmlDocument() {
        System.out.println("getXmlDocument");
        String xml = "";
        Document expResult = null;
        Document result = Tool.getXmlDocument(xml);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getYearMonthStr method, of class Tool.
     */
       @Ignore
    @Test
    public void testGetYearMonthStr() {
        System.out.println("getYearMonthStr");
        Date date = null;
        String expResult = "";
        String result = Tool.getYearMonthStr(date);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of serializeObject method, of class Tool.
     */
       @Ignore
    @Test
    public void testSerializeObject() throws Exception {
        System.out.println("serializeObject");
        Object object = null;
        byte[] expResult = null;
        byte[] result = Tool.serializeObject(object);
        assertArrayEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of deserializeObject method, of class Tool.
     */
       @Ignore
    @Test
    public void testDeserializeObject() throws Exception {
        System.out.println("deserializeObject");
        byte[] buf = null;
        Object expResult = null;
        Object result = Tool.deserializeObject(buf);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getStrFromList method, of class Tool.
     */
       @Ignore
    @Test
    public void testGetStrFromList() {
        System.out.println("getStrFromList");
        List<String> list = null;
        String expResult = "";
        String result = Tool.getStrFromList(list);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getListFromStr method, of class Tool.
     */
       @Ignore
    @Test
    public void testGetListFromStr() {
        System.out.println("getListFromStr");
        String str = "";
        List<String> expResult = null;
//        List<String> result = Tool.getListFromStr(str);
//        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getDurationString method, of class Tool.
     */
    @Ignore
    @Test
    public void testGetDurationString() {
        System.out.println("getDurationString");
        int seconds = 0;
        String expResult = "";
        String result = Tool.getDurationString(seconds);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of twoDigitString method, of class Tool.
     */
    @Ignore
    @Test
    public void testTwoDigitString() {
        System.out.println("twoDigitString");
        int number = 0;
        String expResult = "";
        String result = Tool.twoDigitString(number);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of formalizeIntLongString method, of class Tool.
     */
  @Ignore
    @Test
    public void testFormalizeIntLongString() {
        System.out.println("formalizeIntLongString");
        String value = " 00000000000026397176.";
 
        String result = Tool.formalizeIntLongString(value);
  
        long l = Long.parseLong(result);
    }

    /**
     * Test of removeAroundQuotation method, of class Tool.
     */
      @Ignore
    @Test
    public void testRemoveAroundQuotation() {
        System.out.println("removeAroundQuotation");
        String str = "";
        String expResult = "";
        String result = Tool.removeAroundQuotation(str);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getRandomMilliSeconds method, of class Tool.
     */
      @Ignore
    @Test
    public void testGetRandomMilliSeconds() {
        System.out.println("getRandomMilliSeconds");
        int maxMilliSeconds = 0;
        int expResult = 0;
        int result = Tool.getRandomMilliSeconds(maxMilliSeconds);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getEndDatetime method, of class Tool.
     */
      @Ignore
    @Test
    public void testGetEndDatetime() {
        System.out.println("getEndDatetime");
        String datetimeStr = "";
        String expResult = "";
        String result = Tool.getEndDatetime(datetimeStr);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of convertStringToDateFromES method, of class Tool.
     */
    
    @Ignore
    @Test
    public void testConvertStringToDateFromES() {
        System.out.println("convertStringToDateFromES");
        String dateStr = "2014-05-16T16:02:33.000Z";
 
        long result = Tool.convertESDateStringToLong(dateStr);
        System.out.printf("resutl =%d", result);
        
        dateStr = "2014-05-16T16:03:33.000Z";

        result = Tool.convertESDateStringToLong(dateStr);
        System.out.printf("resutl =%d", result);
    }


   
}
