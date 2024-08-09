package com.colak.jet.kafkaconnect.coucbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;

import java.time.Duration;

class CreateRecord {

    private static String connectionString = "couchbase://localhost";
    private static String username = "admin";
    private static String password = "password";
    private static String bucketName = "travel-sample";

    public static void main(String... args) {
        Cluster cluster = Cluster.connect(
                connectionString,
                ClusterOptions.clusterOptions(username, password).environment(env -> {
                    // Customize client settings by calling methods on the "env" variable.
                })
        );

        // get a bucket reference
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));

        // get a user-defined collection reference
        Scope scope = bucket.scope("_default");
        Collection collection = scope.collection("_default");

        // Upsert Document
        MutationResult upsertResult = collection.upsert(
                "my-document",
                JsonObject.create().put("name", "mike")
        );

        // // Get Document
        // GetResult getResult = collection.get("my-document");
        // String name = getResult.contentAsObject().getString("name");
        // System.out.println(name); // name == "mike"
        //
        // // Call the query() method on the scope object and store the result.
        // Scope inventoryScope = bucket.scope("inventory");
        // QueryResult result = inventoryScope.query("SELECT * FROM airline WHERE id = 10;");
        //
        // // Return the result rows with the rowsAsObject() method and print to the terminal.
        // System.out.println(result.rowsAsObject());
    }
}
