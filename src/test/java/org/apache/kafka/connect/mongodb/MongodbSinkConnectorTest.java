package org.apache.kafka.connect.mongodb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

public class MongodbSinkConnectorTest {
	private MongodbSinkConnector connector;
    private ConnectorContext context;
    private Map<String, String> sinkProperties;
    
    @Before
    public void setup() {
        connector = new MongodbSinkConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        sinkProperties = new HashMap<>();
        sinkProperties.put("host", "localhost");
        sinkProperties.put("port", Integer.toString(27017));
        sinkProperties.put("bulk.size", Integer.toString(100));
        sinkProperties.put("mongodb.database", "mydb");
        sinkProperties.put("mongodb.collections", "test1,test2,test3");
        sinkProperties.put("topics", "test1,test2,test3");
        sinkProperties.put("ids", "id1,id2,id3");

    }
    
    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("localhost", taskConfigs.get(0).get("host"));
        Assert.assertEquals("27017", taskConfigs.get(0).get("port"));
        Assert.assertEquals("100", taskConfigs.get(0).get("bulk.size"));
        Assert.assertEquals("mydb", taskConfigs.get(0).get("mongodb.database"));
        Assert.assertEquals("test1,test2,test3", taskConfigs.get(0).get("mongodb.collections"));
        Assert.assertEquals("test1,test2,test3", taskConfigs.get(0).get("topics"));
        PowerMock.verifyAll();
    }
    
    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(sinkProperties);
        Assert.assertEquals(MongodbSinkTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }
}
