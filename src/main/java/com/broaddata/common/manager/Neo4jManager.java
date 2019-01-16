package com.broaddata.common.manager;

import com.broaddata.common.model.enumeration.RelationshipDirection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import com.broaddata.common.model.vo.Entity;
import com.broaddata.common.model.vo.EntityRelationship;

public class Neo4jManager 
{
    Driver driver;
    Session session;
    private Map<Long, Node> nodes = new HashMap<Long, Node>();
    private Map<Long, Relationship> relationships = new HashMap<Long, Relationship>();
    Map<Long, Entity> entities = new HashMap<Long, Entity>();
    List<EntityRelationship> entityRelationships = new ArrayList<EntityRelationship>();

    public Neo4jManager() {
    }
     
    //public Neo4jManager(String query) {
    //    query(query);
    //}

    public void init(String url, String username, String password) 
    {
        driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));
        session = driver.session();
    }

    public void close() 
    {
        if (session != null)
            session.close();
     
        if (driver != null)
            driver.close();
    }

    public StatementResult run(String query) 
    {
        return session.run(query);
    }

    public void query(String query) 
    {
        StatementResult result = run(query);
        while (result.hasNext()) {
                Record record = result.next();
                List<Value> values = record.values();
                for (Value value : values) {
                        if (value instanceof NodeValue) {
                                Node node = value.asNode();
                                addNode(node);
                        } else if (value instanceof RelationshipValue) {
                                Relationship relationship = value.asRelationship();
                                addRelationship(relationship);
                        } else if (value instanceof PathValue) {
                                Path path = value.asPath();
                                Iterable<Node> nodes = path.nodes();
                                for (Node node : nodes) {
                                        addNode(node);
                                }
                                Iterable<Relationship> relationships = path.relationships();
                                for (Relationship relationship : relationships) {
                                        addRelationship(relationship);
                                }
                        } else if (value instanceof ListValue) {
                                List<Object> list = value.asList();
                                for (Object o : list) {
                                        if (o instanceof InternalRelationship) {
                                                Relationship relationship = (InternalRelationship) o;
                                                addRelationship(relationship);
                                        } else if (o instanceof InternalNode) {
                                                Node node = (InternalNode) o;
                                                addNode(node);
                                        } else if (o instanceof InternalPath) {
                                                Path path = (InternalPath) o;
                                                Iterable<Node> nodes = path.nodes();
                                                for (Node node : nodes) {
                                                        addNode(node);
                                                }
                                                Iterable<Relationship> relationships = path.relationships();
                                                for (Relationship relationship : relationships) {
                                                        addRelationship(relationship);
                                                }
                                        }
                                }
                        } else {
                                throw new RuntimeException("Unsupported query.");
                        }
                }
        }
        
        analysis();
    }

    public String getPropertiesString(String name,Object value) {
            Map<String, Object> properties=new HashMap<String, Object>();
            properties.put(name, value);
            return getPropertiesString(properties);
    }

    public String getPropertiesString(Map<String, Object> properties) {
            StringBuffer buffer = new StringBuffer();
            if (properties!=null&&properties.size()>0) {
                    buffer.append("{ ");
                    for(Entry<String,Object> entry:properties.entrySet()) {
                            buffer.append(entry.getKey());
                            Object value=entry.getValue();
                            if (value instanceof String|value instanceof Date) {
                                    buffer.append(":'").append(value).append("',");
                            } else {
                                    buffer.append(":").append(value).append(",");					
                            }
                    }
                    buffer.delete(buffer.length() - 1, buffer.length());
                    buffer.append("}");
            }
            return buffer.toString();
    }

    public String getQueryNode(String nodeId,String alias) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("(").append(alias).append(" { ").append(Entity.ID).append(": '").append(nodeId).append("' }").append(")");
            return buffer.toString();
    }

    public String getDeleteNodeQuery(String nodeId) {
            StringBuffer buffer = new StringBuffer();
            // MATCH (n:Person { id: '1' }) DETACH DELETE n
            Map<String, Object> properties=new HashMap<String, Object>();
            properties.put(Entity.ID, nodeId);
            buffer.append("MATCH (n").append(getPropertiesString(properties)).append(") DETACH DELETE n") ;
            return buffer.toString();		
    }

    public String getDeleteRelationshipsQuery(String fromNodeId,String toNodeId,List<String> relationshipTypes,RelationshipDirection direction) {
            StringBuffer buffer = new StringBuffer();
            // MATCH ({ id: '1' })-[r]-({ id: '2' }) DELETE r
            Map<String, Object> properties=new HashMap<String, Object>();
            properties.put(Entity.ID, fromNodeId);
            buffer.append("MATCH (").append(getPropertiesString(properties)).append(")") ;
            if (direction == RelationshipDirection.INCOMING) {
                    buffer.append("<-");
            } else {
                    buffer.append("-");
            }
            buffer.append("[r");
            if (relationshipTypes != null && relationshipTypes.size() > 0) {
                    buffer.append(":");
                    for (String relationshipType : relationshipTypes) {
                            buffer.append(relationshipType).append("|");
                    }
                    buffer.delete(buffer.length() - 1, buffer.length());
            }
            buffer.append("]");
            if (direction == RelationshipDirection.OUTGOING) {
                    buffer.append("->");
            } else {
                    buffer.append("-");
            }
            properties=new HashMap<String, Object>();
            properties.put(Entity.ID, toNodeId);
            buffer.append("(").append(getPropertiesString(properties)).append(")") ;		
            buffer.append(" DELETE r");
            return buffer.toString();		
    }

    public String getGetNodeQuery(String nodeId) {
            StringBuffer buffer = new StringBuffer();
            // MATCH (n { id: '1' }) RETURN n
            Map<String, Object> properties=new HashMap<String, Object>();
            properties.put(Entity.ID, nodeId);
            buffer.append("MATCH (n").append(getPropertiesString(properties)).append(") RETURN n") ;
            return buffer.toString();		
    }

    public String getCreateNodeQuery(String nodeType, String nodeId, Map<String, Object> properties) {
            StringBuffer buffer = new StringBuffer();
            // CREATE (n:Person { id: '0' }) RETURN n
            if (properties==null) properties=new HashMap<String, Object>();
            properties.put(Entity.ID, nodeId);
            buffer.append("CREATE (n:").append(nodeType).append(" ").append(getPropertiesString(properties)).append(") RETURN n") ;
            return buffer.toString();		
    }

    public String getAddRelationshipQuery(String outgoingNodeId, String incomingNodeId, boolean isBiDirection,
                    String relationshipType, Map<String, Object> properties) {
            StringBuffer buffer = new StringBuffer();
            // MATCH (a:Person { id: '1' }), (b:Person { id: '2' }) CREATE (a)-[:RELATIONSHIP]->(b)
            buffer.append("MATCH ").append(getQueryNode(outgoingNodeId,"a"));
            buffer.append(", ").append(getQueryNode(incomingNodeId,"b"));
            String relationProperties=getPropertiesString(properties);
            buffer.append(" CREATE ");
            buffer.append("(a)-[:").append(relationshipType).append(relationProperties).append("]->(b)");
            if (isBiDirection) {
                    buffer.append(",").append("(b)-[:").append(relationshipType).append(relationProperties).append("]->(a)");
            }
            return buffer.toString();
    }	

    public String getRelationQuery(List<String> relationshipTypeFilter, int maxNetworkLevel,
                    RelationshipDirection direction) {
            StringBuffer buffer = new StringBuffer();
            if (direction == RelationshipDirection.INCOMING) {
                    buffer.append("<-");
            } else {
                    buffer.append("-");
            }
            buffer.append("[r");
            if (relationshipTypeFilter != null && relationshipTypeFilter.size() > 0) {
                    buffer.append(":");
                    for (String relationshipType : relationshipTypeFilter) {
                            buffer.append(relationshipType).append("|");
                    }
                    buffer.delete(buffer.length() - 1, buffer.length());
            }
            if (maxNetworkLevel > 1) {
                    buffer.append("*1..").append(maxNetworkLevel);
            }
            buffer.append("]");
            if (direction == RelationshipDirection.OUTGOING) {
                    buffer.append("->");
            } else {
                    buffer.append("-");
            }
            return buffer.toString();		
    }

    public String getNodesQuery(List<String> nodeIds, List<String> relationshipTypeFilter, int maxNetworkLevel,
                    RelationshipDirection direction) {
            StringBuffer buffer = new StringBuffer();
            // MATCH (n)-[r]-(neighbors) WHERE n.id IN ['0', '1'] RETURN n,neighbors,r
            buffer.append("MATCH (n)");
            buffer.append(getRelationQuery(relationshipTypeFilter,maxNetworkLevel,direction));
            buffer.append("(neighbors) WHERE n.id IN [");
            for(String nodeId:nodeIds) {
                    buffer.append("'").append(nodeId).append("',");
            }
            buffer.delete(buffer.length() - 1, buffer.length());
            buffer.append("] RETURN n,r,neighbors");
            return buffer.toString();
    }

    public String getQuery(String nodeId, List<String> relationshipTypeFilter, int maxNetworkLevel,
                    RelationshipDirection direction) {
            StringBuffer buffer = new StringBuffer();
            // MATCH (n { ID: '1' })-[r]-(neighbors) RETURN n,neighbors,r
            buffer.append("MATCH ").append(getQueryNode(nodeId,"n"));
            buffer.append(getRelationQuery(relationshipTypeFilter,maxNetworkLevel,direction));
            buffer.append("(neighbors) RETURN n,r,neighbors");
            return buffer.toString();
    }

    public void analysis() 
    {
        for (Entry<Long, Node> entry : nodes.entrySet()) {
                Node node = entry.getValue();
                Map<String,Object> properties=new HashMap<String,Object>();
                properties.putAll(node.asMap());
                Entity entity = new Entity(node.labels().iterator().next(), properties);
                entities.put(entry.getKey(), entity);
        }
        for (Entry<Long, Relationship> entry : relationships.entrySet()) {
                Relationship relationship = entry.getValue();
                EntityRelationship entityRelationship = new EntityRelationship(relationship.type());
                Map<String,Object> properties=new HashMap<String,Object>();
                properties.putAll(relationship.asMap());
                entityRelationship.setProperties(properties);
                Entity startEntity = entities.get(relationship.startNodeId());
                Entity endEntity = entities.get(relationship.endNodeId());
                entityRelationship.setStart(Entity.getLightEntity(startEntity));
                entityRelationship.setEnd(Entity.getLightEntity(endEntity));
                startEntity.addOutRelstionship(entityRelationship);
                endEntity.addInRelstionship(entityRelationship);
                entityRelationships.add(entityRelationship);
        }
    }

    public List<EntityRelationship> getEntityRelationships() {
            return entityRelationships;
    }

    public List<Entity> getEntities() {
            return new ArrayList<Entity>(entities.values());
    }

    public void addNode(Node node) {
            nodes.put(node.id(), node);
    }

    public void addRelationship(Relationship relationship) {
            relationships.put(relationship.id(), relationship);
    }
}
