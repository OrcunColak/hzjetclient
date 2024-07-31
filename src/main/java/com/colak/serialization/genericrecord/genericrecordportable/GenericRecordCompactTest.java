package com.colak.serialization.genericrecord.genericrecordportable;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenericRecordCompactTest {

    public static void main(String[] args) {
        GenericRecord genericRecord = GenericRecordBuilder.compact("person")
                .setInt32("id", 1)
                .setString("firstName", "a")
                .setString("lastName", "b")
                .build();

        // GenericRecord : {"person": {"firstName": "a", "id": 1, "lastName": "b"}}
        log.info("GenericRecord : {}", genericRecord);
    }
}