 
package com.broaddata.common.util;

import com.broaddata.common.manager.GraphDatabaseManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.broaddata.common.model.enumeration.RelationshipDirection;
import com.broaddata.common.model.vo.Entity;
import com.broaddata.common.model.vo.EntityRelationship;

/**
 *
 * @author edf
 */
public class GraphDatabaseUtilTest {
	static Random random = new Random();
	private static String[] cities = { "北京", "天津", "上海", "重庆", "沈阳", "南京", "杭州", "成都" };
	private static String[] sexes = { "男", "女" };
	private static Boolean[] married = { true, false };
	private static Integer[] ages = { 25, 28, 30, 35, 39, 40, 43, 47, 49, 53 };
	private static Double[] heightes = { 162.5, 165.4, 169.1, 170.5, 172.8, 175.9, 180.8, 185.4 };
	private static String[] firstNames = { "辰逸", "浩宇", "瑾瑜", "皓轩", "擎苍", "擎宇", "烨磊", "晟睿", "文博", "天佑", "英杰", "致远", "俊驰",
			"雨泽", "烨磊", "伟奇", "晟睿", "文博", "天佑", "文昊", "修洁", "黎昕", "远航", "旭尧", "圣杰", "俊楠", "鸿涛", "伟祺", "荣轩", "越泽", "浩宇",
			"瑾瑜", "皓轩", "擎苍", "擎宇", "志泽" };
	private static String[] lastNames = { "李", "王", "张", "刘", "陈", "杨", "黄", "赵", "周", "吴", "徐", "孙", "朱", "马", "胡",
			"郭", "林", "何", "高", "梁", "郑", "罗", "宋", "谢", "唐", "韩", "曹", "许", "邓", "萧", "冯", "曾", "程", "蔡", "彭", "潘",
			"袁", "于", "董", "余", "苏", "叶", "吕", "魏", "蒋", "田", "杜", "丁", "沈", "姜", "范", "江", "傅", "钟", "卢", "汪", "戴",
			"崔", "任", "陆", "廖", "姚", "方", "金", "邱", "夏", "谭", "韦", "贾", "邹", "石", "熊", "孟", "秦", "阎", "薛", "侯", "雷",
			"白", "龙", "殷", "郝", "孔", "邵", "史", "毛", "常", "万", "顾", "赖", "武", "康", "贺", "严", "尹", "钱", "施", "牛", "洪",
			"龚" };
	static String[] relations = { "KNOWS", "TOWNEE", "SCHOOLMATE", "RELATIVE" };
	static String[] bands = { "Benz", "BMW", "Chevelot", "Tesla" };
	private static String[] engines = { "1.8", "1.6", "2.0", "1.4T", "1.6T" };

	public static void main(String[] args) 
        {
                GraphDatabaseManager gdManager = new GraphDatabaseManager();
                   
		gdManager.init("bolt://localhost", "neo4j", "haierkt");
		
                try {
			generateData(gdManager);
			listEntitiesAndRelations(gdManager);
			testFunctions(gdManager);
		} catch (Exception e) {
			e.printStackTrace();
		}
                
		gdManager.close();
	}

	public static void listEntitiesAndRelations(GraphDatabaseManager gdManager) throws Exception {
		for (int i = 0; i < 1000; i++) {
			System.out.println(gdManager.getEntity(i + "").getDisplayString());
			printRelationships(gdManager.getOneEntityRelationshipList(i + ""));
		}
		for (int i = 10000; i < 10100; i++) {
			System.out.println(gdManager.getEntity(i + "").getDisplayString());
			printRelationships(gdManager.getOneEntityRelationshipList(i + ""));
		}
	}

	public static void testFunctions(GraphDatabaseManager gdManager) throws Exception {
		System.out.println(gdManager.getEntity("0").getDisplayString());
		printRelationships(gdManager.getOneEntityRelationshipList("1", "KNOWS"));
		printRelationships(gdManager.getOneEntityRelationshipList("1", "KNOWS", 2, RelationshipDirection.BOTH));
		printRelationships(gdManager.getOneEntityRelationshipList("1", new String[] { "KNOWS", "RELATIVE" }));
		printRelationships(gdManager.getOneEntityRelationshipList("1", new String[] { "KNOWS", "RELATIVE" }, 3,
				RelationshipDirection.OUTGOING));
		printRelationships(gdManager.getOneEntityRelationshipList("1", new String[] { "KNOWS", "RELATIVE" }, 2,
				RelationshipDirection.BOTH));
		System.out.println(gdManager.getOneEntityRelationships("1").getDisplayString());
		System.out.println(gdManager.getOneEntityRelationships("1", "KNOWS").getDisplayString());
		System.out.println(gdManager.getOneEntityRelationships("1", "KNOWS", 2, RelationshipDirection.BOTH)
				.getDisplayString());
		System.out.println(gdManager
				.getOneEntityRelationships("1", new String[] { "KNOWS", "RELATIVE" }, 3, RelationshipDirection.OUTGOING)
				.getDisplayString());
		printRelationships(gdManager.getMutilpleEntityRelationshipList(new String[] { "0", "1" }));
		printRelationships(gdManager.getMutilpleEntityRelationshipList(new String[] { "0", "1" },
				new String[] { "KNOWS", "SCHOOLMATE" }));
		printRelationships(gdManager.getMutilpleEntityRelationshipList(new String[] { "0", "1" },
				new String[] { "KNOWS", "SCHOOLMATE" }, 2, RelationshipDirection.OUTGOING));
		printEntities(gdManager.getMutipleEntityRelationships(new String[] { "0", "1" }));
		printEntities(gdManager.getMutipleEntityRelationships(new String[] { "0", "1" },
				new String[] { "KNOWS", "SCHOOLMATE" }));
		printEntities(gdManager.getMutipleEntityRelationships(new String[] { "0", "1" },
				new String[] { "KNOWS", "SCHOOLMATE" }, 2, RelationshipDirection.OUTGOING));
	}

	private static Object getRandomValue(Object[] values) {
		return values[random.nextInt(values.length)];
	}

	private static Map<String, Object> getPersonProperties() {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("name", (String) getRandomValue(lastNames) + getRandomValue(firstNames));
		properties.put("sex", getRandomValue(sexes));
		properties.put("city", getRandomValue(cities));
		properties.put("married", getRandomValue(married));
		properties.put("age", getRandomValue(ages));
		properties.put("height", getRandomValue(heightes));
		return properties;
	}

	private static Map<String, Object> getCarProperties() {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("band", getRandomValue(bands));
		properties.put("engine", getRandomValue(engines));
		return properties;
	}

	public static void generateData(GraphDatabaseManager gdManager) throws Exception {
		for (int i = 0; i < 1000; i++) {
			gdManager.createEntity("Person", i + "", getPersonProperties());
		}
		for (int i = 10000; i < 10100; i++) {
			gdManager.createEntity("Car", i + "", getCarProperties());
		}
		for (int i = 0; i < relations.length; i++) {
			int length = random.nextInt(400);
			for (int j = 0; j < length; j++) {
				gdManager.addRelationship(random.nextInt(1000) + "", random.nextInt(1000) + "",
						random.nextInt(1000) % 2 == 0, relations[i], null);
			}
		}
		for (int i = 10000; i < 10100; i++) {
			gdManager.addRelationship(random.nextInt(1000) + "", i + "", false, "OWN", null);
		}
	}

	public static void printRelationships(List<EntityRelationship> entityRelationships) {
		for (EntityRelationship entityRelationship : entityRelationships) {
			System.out.println(entityRelationship);
		}

	}

	public static void printEntities(List<Entity> entities) {
		for (Entity entity : entities) {
			System.out.println(entity.getDisplayString());
		}

	}
}
