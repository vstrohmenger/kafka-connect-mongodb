package org.apache.kafka.connect.mongodb;

import java.io.File;
import java.util.*;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.bson.Document;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import junit.framework.TestCase;

public class MongodbSinkTaskTest extends TestCase {
	//private static String REPLICATION_PATH = "/home/vagrant/mongo";
	
	private MongodbSinkTask task;
	private OffsetStorageReader offsetStorageReader;
    private SinkTaskContext context;
	private Map<String, String> sinkProperties;
	
	private MongodExecutable mongodExecutable;
    private MongodProcess mongod;
    private MongodStarter mongodStarter;
    private IMongodConfig mongodConfig;
    private MongoClient mongoClient;
	
    private SchemaBuilder getValueSchemaBuilder(Schema keySchema)
    {
    	return SchemaBuilder.struct().name("valueSchema")
				.field("idt", keySchema)
				.field("name", Schema.STRING_SCHEMA);
    }
    private SchemaBuilder getKeySchemaBuilder(Schema keySchema)
    {
    	return SchemaBuilder.struct().name("keySchema")
				.field("idt",  keySchema);
    }
    private SchemaBuilder getStructSchemaBuilder(Schema keySchema)
    {
    	return SchemaBuilder.struct().name("valueSchema")
				.field("idt", keySchema)
				.field("name",SchemaBuilder.array(Schema.STRING_SCHEMA).build());
    }
    
	@Override
    public void setUp() {
		
		try {
            super.setUp();
          /*  mongodStarter = MongodStarter.getDefaultInstance();
            mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.V3_2)
                    //.replication(new Storage(REPLICATION_PATH, "rs0", 1024))
                    .net(new Net(27017, Network.localhostIsIPv6()))
                    .build();
            mongodExecutable = mongodStarter.prepare(mongodConfig);
            mongod = mongodExecutable.start();*/
            mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
            //MongoDatabase adminDatabase = mongoClient.getDatabase("admin");

          /*  BasicDBObject replicaSetSetting = new BasicDBObject();
            replicaSetSetting.put("_id", "rs0");
            BasicDBList members = new BasicDBList();
            DBObject host = new BasicDBObject();
            host.put("_id", 0);
            host.put("host", "127.0.0.1:27017");
            members.add(host);
            replicaSetSetting.put("members", members);
            adminDatabase.runCommand(new BasicDBObject("isMaster", 1));
            adminDatabase.runCommand(new BasicDBObject("replSetInitiate", replicaSetSetting));*/
            MongoDatabase db = mongoClient.getDatabase("mydb");
            db.createCollection("sink1");
            db.createCollection("sink2");
            db.createCollection("sink3");
        } catch (Exception e) {
//                Assert.assertTrue(false);
        }
		
		task = new MongodbSinkTask();

		offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        context = PowerMock.createMock(SinkTaskContext.class);
        task.initialize(context);
		
		sinkProperties = new HashMap<>();
        sinkProperties.put("host", "localhost");
        sinkProperties.put("port", Integer.toString(27017));
        sinkProperties.put("bulk.size", Integer.toString(100));
        sinkProperties.put("mongodb.database", "mydb");
        sinkProperties.put("mongodb.collections", "sink1,sink2,sink3");
        sinkProperties.put("topics", "test1,test2,test3");      
	}
	
	@Test
    public void testInsertWithId() {
		//Verification that the record as well been added.
		MongoDatabase db = mongoClient.getDatabase("mydb");
		if (db.getCollection("sink1")!=null)
		{
			db.getCollection("sink1").drop();
		}
		
		//Creation of the test data
		String topic = "test1";
		int partition = 1; 
		Schema keySchema = getKeySchemaBuilder(Schema.INT32_SCHEMA).build();
		Object key=new Struct(keySchema).put("idt",10);
		Schema valueSchema=getValueSchemaBuilder(Schema.INT32_SCHEMA).build();
		Object value=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", "John");
		long offset=20;
		
        sinkProperties.put("ids", "test1#idt,test2#idt2,test3"); 
		task.start(sinkProperties);
		Collection<SinkRecord> newCollection = new ArrayList<SinkRecord>();
		SinkRecord record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		
		
		Assert.assertNotNull(db.getCollection("sink1"));
		Assert.assertTrue(db.getCollection("sink1").count()==1);
		Assert.assertTrue(db.getCollection("sink1").find().first().get("_id", Integer.class)==10);
		Assert.assertTrue(db.getCollection("sink1").find().first().getString("name").compareTo("John")==0);
	}
	
	@Test
    public void testInsertWithStruct() {
		//Verification that the record as well been added.
		MongoDatabase db = mongoClient.getDatabase("mydb");
		if (db.getCollection("sink1")!=null)
		{
			db.getCollection("sink1").drop();
		}
		
		//Creation of the test data
		String topic = "test1";
		int partition = 1; 
		Schema keySchema = getKeySchemaBuilder(Schema.INT32_SCHEMA).build();
		Object key=new Struct(keySchema).put("idt",10);
		Schema valueSchema=getStructSchemaBuilder(Schema.INT32_SCHEMA).build();
		Object value=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", new ArrayList<String>(){{add("1");add("3");add("5");add("7");add("9");}});
		long offset=20;
		
        sinkProperties.put("ids", "test1#idt"); 
		task.start(sinkProperties);
		Collection<SinkRecord> newCollection = new ArrayList<SinkRecord>();
		SinkRecord record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		
		
		Assert.assertNotNull(db.getCollection("sink1"));
		Assert.assertTrue(db.getCollection("sink1").count()==1);
		Assert.assertTrue(db.getCollection("sink1").find().first().get("_id", Integer.class)==10);
		Assert.assertTrue(db.getCollection("sink1").find().first().get("name", ArrayList.class).size()==5);
	}
	
	@Test
    public void testInsertWithStringId() {
		//Verification that the record as well been added.
		MongoDatabase db = mongoClient.getDatabase("mydb");
		if (db.getCollection("sink1")!=null)
		{
			db.getCollection("sink1").drop();
		}
		
		//Creation of the test data
		String topic = "test1";
		int partition = 1; 
		Schema keySchema = getKeySchemaBuilder(Schema.STRING_SCHEMA).build();
		Object key=new Struct(keySchema).put("idt","monId");
		Schema valueSchema=getValueSchemaBuilder(Schema.STRING_SCHEMA).build();
		Object value=new Struct(valueSchema)
				.put("idt", "monId")
				.put("name", "John");
		long offset=20;
		
        sinkProperties.put("ids", "test1#idt,test2#idt2,test3"); 
		task.start(sinkProperties);
		Collection<SinkRecord> newCollection = new ArrayList<SinkRecord>();
		SinkRecord record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		
		
		Assert.assertNotNull(db.getCollection("sink1"));
		Assert.assertTrue(db.getCollection("sink1").count()==1);
		Assert.assertTrue(db.getCollection("sink1").find().first().get("_id", String.class).compareTo("monId")==0);
		Assert.assertTrue(db.getCollection("sink1").find().first().getString("name").compareTo("John")==0);
	}
	
	@Test
    public void testInsertWithNullId() {
		//Verification that the record as well been added.
		MongoDatabase db = mongoClient.getDatabase("mydb");
		if (db.getCollection("sink1")!=null)
		{
			db.getCollection("sink1").drop();
		}
		
		//Creation of the test data
		String topic = "test1";
		int partition = 1; 
		Schema keySchema = getKeySchemaBuilder(Schema.INT32_SCHEMA).build();
		Object key=new Struct(keySchema);
		Schema valueSchema=getValueSchemaBuilder(Schema.INT32_SCHEMA).build();
		Object value=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", "John");
		long offset=20;
		
        sinkProperties.put("ids", "test1,test2,test3"); 
		task.start(sinkProperties);
		Collection<SinkRecord> newCollection = new ArrayList<SinkRecord>();
		SinkRecord record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		
		
		Assert.assertNotNull(db.getCollection("sink1"));
		Assert.assertTrue(db.getCollection("sink1").count()==1);
		Assert.assertTrue(db.getCollection("sink1").find().first().getLong("_id")==record.kafkaOffset());
		Assert.assertTrue(db.getCollection("sink1").find().first().getString("name").compareTo("John")==0);
	}
	
	@Test
    public void testUpdateWithId() {
		//Verification that the record as well been added.
		MongoDatabase db = mongoClient.getDatabase("mydb");
		if (db.getCollection("sink1")!=null)
		{
			db.getCollection("sink1").drop();
		}
		
		//Creation of the test data
		String topic = "test1";
		int partition = 1; 
		Schema keySchema = getKeySchemaBuilder(Schema.INT32_SCHEMA).build();
		Object key=new Struct(keySchema).put("idt",10);
		Schema valueSchema=getValueSchemaBuilder(Schema.INT32_SCHEMA).build();
		Object value=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", "John");
		long offset=20;
		
        sinkProperties.put("ids", "test1#idt,test2#idt2,test3"); 
		task.start(sinkProperties);
		Collection<SinkRecord> newCollection = new ArrayList<SinkRecord>();
		SinkRecord record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		
		value=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", "James");
        sinkProperties.put("ids", "test1#idt,test2#idt2,test3"); 
		task.start(sinkProperties);
		newCollection = new ArrayList<SinkRecord>();
		record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		Assert.assertNotNull(db.getCollection("sink1"));
		MongoCollection<Document> collection = db.getCollection("sink1");
		Assert.assertTrue(collection != null);
		Assert.assertTrue(collection.find().first()!=null);
		Assert.assertTrue(collection.find().first().get("_id", Integer.class)==10);
		Assert.assertTrue(collection.find().first().getString("name").compareTo("James")==0);
	}
	
	@Test
    public void testUpdateWithNullId() throws Exception {
		//Verification that the record as well been added.
		MongoDatabase db = mongoClient.getDatabase("mydb");
		if (db.getCollection("sink1")!=null)
		{
			db.getCollection("sink1").drop();
		}
		
		//Creation of the test data
		String topic = "test1";
		int partition = 1; 
		Schema keySchema = getKeySchemaBuilder(Schema.INT32_SCHEMA).build();
		Object key=new Struct(keySchema).put("idt",10);
		Schema valueSchema=getValueSchemaBuilder(Schema.INT32_SCHEMA).build();
		Object value=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", "John");
		long offset=20;
		
        sinkProperties.put("ids", "test1#idt,test2#idt2,test3"); 
		task.start(sinkProperties);
		Collection<SinkRecord> newCollection = new ArrayList<SinkRecord>();
		SinkRecord record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		valueSchema=new Struct(valueSchema)
				.put("idt", 10)
				.put("name", "James").schema();
				
        sinkProperties.put("ids", "test1,test2,test3");
        
        
		task.start(sinkProperties);
		newCollection = new ArrayList<SinkRecord>();
		record = new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
		newCollection.add(record);
		
		//Insertion of test data in Mongodb collection
		task.put(newCollection);
		Assert.assertNotNull(db.getCollection("sink1"));
		Assert.assertTrue(db.getCollection("sink1").count()==2);
	}
	
	@Override
    public void tearDown() {
        try {
            super.tearDown();
            mongod.stop();
            mongodExecutable.stop();
            System.out.println("DELETING OPLOG");
            //FileUtils.deleteDirectory(new File(REPLICATION_PATH));
        } catch (Exception e) {
        }
    }
}
