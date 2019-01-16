/*
 * GraphDatabaseUtil.java
 */

package com.broaddata.common.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import java.util.List;

import com.broaddata.common.model.vo.Entity;
import com.broaddata.common.model.vo.EntityRelationship;
import com.broaddata.common.model.enumeration.RelationshipDirection;

public class GraphDatabaseManager 
{
    private final Neo4jManager neo4jManager = new Neo4jManager();
    
    public GraphDatabaseManager() {   
    }
    
    public void init(String url, String username, String password)
    {
        neo4jManager.init(url, username,password);
    }
    
    public void close()
    {
        neo4jManager.close();
    }
    
    public Entity createEntity(String nodeType, String nodeId, Map<String, Object> properties) throws Exception {
            String query = neo4jManager.getCreateNodeQuery(nodeType, nodeId, properties);
            neo4jManager.run(query);
            Entity entity = new Entity(nodeType, nodeId, null, properties);
            return entity;
    }

    public void deleteEntity(String nodeId) throws Exception {
            String query = neo4jManager.getDeleteNodeQuery(nodeId);
            neo4jManager.run(query);
    }


    public Entity getEntity(String nodeId) throws Exception {
            String query = neo4jManager.getGetNodeQuery(nodeId);
           // Neo4jManager helper = new Neo4jManager(query);
            neo4jManager.query(query);
            List<Entity> entities = neo4jManager.getEntities();
            for (Entity entity : entities) {
                    if (entity.getId().equals(nodeId))
                            return entity;
            }
            return null;
    }

    public void addRelationship(String outgoingNodeId, String incomingNodeId, boolean isBiDirection,
                    String relationshipType, Map<String, Object> properties) throws Exception {
            String query = neo4jManager.getAddRelationshipQuery(outgoingNodeId, incomingNodeId, isBiDirection,
                            relationshipType, properties);
            neo4jManager.run(query);
    }

    public void deleteRelationships(String fromNodeId,String toNodeId) throws Exception {
            deleteRelationships(fromNodeId,toNodeId,(List<String>) null,RelationshipDirection.BOTH);
    }

    public void deleteRelationships(String fromNodeId,String toNodeId,String relationshipType) throws Exception {
            deleteRelationships(fromNodeId,toNodeId,new String[]{relationshipType},RelationshipDirection.BOTH);
    }

    public void deleteRelationships(String fromNodeId,String toNodeId,String relationshipType,RelationshipDirection direction) throws Exception {
            deleteRelationships(fromNodeId,toNodeId,new String[]{relationshipType},direction);
    }

    public void deleteRelationships(String fromNodeId,String toNodeId,String[] relationshipTypes) throws Exception {
            deleteRelationships(fromNodeId,toNodeId,Arrays.asList(relationshipTypes),RelationshipDirection.BOTH);
    }

    public void deleteRelationships(String fromNodeId,String toNodeId,String[] relationshipTypes,RelationshipDirection direction) throws Exception {
            deleteRelationships(fromNodeId,toNodeId,Arrays.asList(relationshipTypes),direction);
    }

    public void deleteRelationships(String fromNodeId,String toNodeId,List<String> relationshipTypes,RelationshipDirection direction) throws Exception {
            String query = neo4jManager.getDeleteRelationshipsQuery(fromNodeId,toNodeId,relationshipTypes,direction);
            neo4jManager.run(query);
    }

    public List<EntityRelationship> getOneEntityRelationshipList(String nodeId) throws Exception {
            return getOneEntityRelationshipList(nodeId, (List<String>) null, 1, RelationshipDirection.BOTH);
    }

    public List<EntityRelationship> getOneEntityRelationshipList(String nodeId, String relationshipType)
                    throws Exception {
            return getOneEntityRelationshipList(nodeId, relationshipType, 1, RelationshipDirection.BOTH);
    }

    public List<EntityRelationship> getOneEntityRelationshipList(String nodeId, String[] relationshipTypes)
                    throws Exception {
            return getOneEntityRelationshipList(nodeId, relationshipTypes, 1, RelationshipDirection.BOTH);
    }

    public List<EntityRelationship> getOneEntityRelationshipList(String nodeId, String[] relationshipTypes,
                    int maxNetworkLevel, RelationshipDirection direction) throws Exception {
            return getOneEntityRelationshipList(nodeId, Arrays.asList(relationshipTypes), maxNetworkLevel, direction);
    }

    public List<EntityRelationship> getOneEntityRelationshipList(String nodeId, String relationshipType,
                    int maxNetworkLevel, RelationshipDirection direction) throws Exception {
            return getOneEntityRelationshipList(nodeId, new String[] { relationshipType }, maxNetworkLevel, direction);
    }

    public List<EntityRelationship> getOneEntityRelationshipList(String nodeId,
                    List<String> relationshipTypeFilter, int maxNetworkLevel, RelationshipDirection direction)
                                    throws Exception {
            //Neo4jManager helper = new Neo4jManager(
            String query = neo4jManager.getQuery(nodeId, relationshipTypeFilter, maxNetworkLevel, direction);
            neo4jManager.query(query);
            return neo4jManager.getEntityRelationships();
    }

    public Entity getOneEntityRelationships(String nodeId) throws Exception {
            return getOneEntityRelationships(nodeId, (List<String>) null, 1, RelationshipDirection.BOTH);
    }

    public Entity getOneEntityRelationships(String nodeId, String relationshipType) throws Exception {
            return getOneEntityRelationships(nodeId, relationshipType, 1, RelationshipDirection.BOTH);
    }

    public Entity getOneEntityRelationships(String nodeId, String relationshipType, int maxNetworkLevel,
                    RelationshipDirection direction) throws Exception {
            return getOneEntityRelationships(nodeId, new String[] { relationshipType }, maxNetworkLevel, direction);
    }

    public Entity getOneEntityRelationships(String nodeId, String[] relationshipTypes, int maxNetworkLevel,
                    RelationshipDirection direction) throws Exception {
            return getOneEntityRelationships(nodeId, Arrays.asList(relationshipTypes), maxNetworkLevel, direction);
    }

    public Entity getOneEntityRelationships(String nodeId, List<String> relationshipTypeFilter,
                    int maxNetworkLevel, RelationshipDirection direction) throws Exception {
            //Neo4jManager helper = new Neo4jManager(
            String query = neo4jManager.getQuery(nodeId, relationshipTypeFilter, maxNetworkLevel, direction);
            
            neo4jManager.query(query);
            
            List<Entity> entities = neo4jManager.getEntities();
            for (Entity entity : entities) {
                    if (entity.getId().equals(nodeId))
                            return entity;
            }
            return null;
    }

    public List<EntityRelationship> getMutilpleEntityRelationshipList(String[] nodeIds) throws Exception {
            return getMutilpleEntityRelationshipList(Arrays.asList(nodeIds), null, 1, RelationshipDirection.BOTH);
    }

    public List<EntityRelationship> getMutilpleEntityRelationshipList(String[] nodeIds,
                    String[] relationshipTypes) throws Exception {
            return getMutilpleEntityRelationshipList(Arrays.asList(nodeIds), Arrays.asList(relationshipTypes), 1,
                            RelationshipDirection.BOTH);
    }

    public List<EntityRelationship> getMutilpleEntityRelationshipList(String[] nodeIds,
                    String[] relationshipTypes, int maxNetworkLevel, RelationshipDirection direction) throws Exception {
            return getMutilpleEntityRelationshipList(Arrays.asList(nodeIds), Arrays.asList(relationshipTypes),
                            maxNetworkLevel, direction);
    }

    public List<EntityRelationship> getMutilpleEntityRelationshipList(List<String> nodeIds,
                    List<String> relationshipTypeFilter, int maxNetworkLevel, RelationshipDirection direction)
                                    throws Exception {
            //Neo4jManager helper = new Neo4jManager(
            String query = neo4jManager.getNodesQuery(nodeIds, relationshipTypeFilter, maxNetworkLevel, direction);
            neo4jManager.query(query);
            return neo4jManager.getEntityRelationships();
    }

    public List<Entity> getMutipleEntityRelationships(String[] nodeIds) throws Exception {
            return getMutipleEntityRelationships(Arrays.asList(nodeIds), null, 1, RelationshipDirection.BOTH);
    }

    public List<Entity> getMutipleEntityRelationships(String[] nodeIds, String[] relationshipTypes)
                    throws Exception {
            return getMutipleEntityRelationships(Arrays.asList(nodeIds), Arrays.asList(relationshipTypes), 1,
                            RelationshipDirection.BOTH);
    }

    public List<Entity> getMutipleEntityRelationships(String[] nodeIds, String[] relationshipTypes,
                    int maxNetworkLevel, RelationshipDirection direction) throws Exception {
            return getMutipleEntityRelationships(Arrays.asList(nodeIds), Arrays.asList(relationshipTypes), maxNetworkLevel,
                            direction);
    }

    public List<Entity> getMutipleEntityRelationships(List<String> nodeIds, List<String> relationshipTypeFilter,
                    int maxNetworkLevel, RelationshipDirection direction) throws Exception {
            //Neo4jManager helper = new Neo4jManager(
            String query = neo4jManager.getNodesQuery(nodeIds, relationshipTypeFilter, maxNetworkLevel, direction);
            neo4jManager.query(query);
            List<Entity> entities = neo4jManager.getEntities();
            List<Entity> result = new ArrayList<Entity>();
            for (Entity entity : entities) {
                    for (String nodeId : nodeIds) {
                            if (entity.getId().equals(nodeId))
                                    result.add(entity);
                    }
            }
            return result;
    }
}
