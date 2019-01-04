package com.loyalty.kafka.couchbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.*;
import com.loyalty.kafka.app.App;
import com.loyalty.kafka.constants.CouchbaseConstants;;

public class CBQuery {
	public Bucket bucket = null;
	Cluster cluster = null;
	private static  Logger logger =null;
	public CBQuery(Logger logger_instance){
		logger = logger_instance;

        try {
            //this tunes the SDK (to customize connection timeout)
            CouchbaseEnvironment env= DefaultCouchbaseEnvironment.builder()
                    .connectTimeout(200000) //10000ms = 10s,50000 = 50s default is 5s
                    .build();
            
            
			logger.info("Create connection");
            //use the env during cluster creation to apply
            this.cluster = CouchbaseCluster.create(env, CouchbaseConstants.COUCHBASE_INSTANCE);
            System.out.println("created cluster");
            this.cluster.authenticate(CouchbaseConstants.USERNAME, CouchbaseConstants.PASSWORD);
            System.out.println("Try to open bucket");

            this.bucket = this.cluster.openBucket(CouchbaseConstants.BUCKET ); //you can also force a greater timeout here (cluster.openBucket("beer-sample", 10, TimeUnit.SECONDS))
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        
    }
//    public static void main(String... args) throws Exception {
////        CBQuery cb_obj = new CBQuery();
//        // Initialize the Connection        
//        JsonObject memberid  = JsonObject.create()
//        		.put("typeCode","CSID")
//        		.put("value","169002641");
//
//        
//        JsonObject value = JsonObject.create()
//                .put("sourceSystemCode","8") 
//                .put("sourceSubsystemCode","ALIBABA")
//                .put("operatorId","ALIBABA")
//                .put("memberId",memberid);
//
//        cb_obj.insert("88888", value.toString());
//       
//        
//        //cb_obj.update();
//       // cb_obj.getId();
//      
//    }
    
    public void getAllDoc (){
        N1qlQueryResult result = bucket.query(
                N1qlQuery.simple("SELECT * FROM `Devpoc` ")
            );

            // Print each found Row
            for (N1qlQueryRow row : result) {
                System.out.println(row);
            }
    }
    
	public void getId( ) {
		System.out.println("GETTINg");
	     String id = "affiliateactvty::1032";
         JsonDocument doc1 = this.bucket.get(id);
		 System.out.println("\n***********************************\n" + doc1);
	}
	
	public String insert(String document_id , String json_str) {
		try{

		System.out.println("CBQUERY: insert" );
		JsonDocument return_id = null;
		JsonObject.create();
		JsonObject document = JsonObject.fromJson(json_str);
		
		JsonDocument doc = JsonDocument.create(document_id,document);
		JsonDocument doc_exists = this.bucket.get(document_id);
		if(doc_exists.id().equals(document_id)){
			return_id = this.bucket.upsert(doc);
		}else 
			return_id = this.bucket.insert(doc);
		System.out.println("CBQUERY: This is the document created : " +return_id.id());
		return return_id.id();

		}catch(Exception e){
			//e.printStackTrace();
			System.out.println("CBQUERY: ERROR  " +e.getLocalizedMessage());

			return null;
		}
	}
	
	public void update() {
		// TODO Auto-generated method stub
		
	}
	public void upSert() {
		
		  // Create a JSON Document
        JsonObject arthur = JsonObject.create()
            .put("name", "Arthur")
            .put("email", "kingarthur@couchbase.com")
            .put("interests", JsonArray.from("Holy Grail", "African Swallows"));

        // Store the Document
        this.bucket.upsert(JsonDocument.create("u:king_arthur", arthur));

        // Load the Document and print it
        // Prints Content and Metadata of the stored Document
        System.out.println(bucket.get("u:king_arthur"));

        // Create a N1QL Primary Index (but ignore if it exists)
        this.bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        // Perform a N1QL Query
        N1qlQueryResult result = bucket.query(
            N1qlQuery.parameterized("SELECT name FROM `bucketname` WHERE $1 IN interests",
            JsonArray.from("African Swallows"))
        );

        // Print each found Row
        for (N1qlQueryRow row : result) {
            // Prints {"name":"Arthur"}
            System.out.println(row);
        }		
	}
}