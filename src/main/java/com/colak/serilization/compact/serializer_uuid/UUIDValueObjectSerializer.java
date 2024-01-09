package com.colak.serilization.compact.serializer_uuid;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import java.util.UUID;

class UUIDValueObjectSerializer implements CompactSerializer<UUIDValueObject> {
    @Override
    public UUIDValueObject read(CompactReader reader) {
        UUIDValueObject valueObject = new UUIDValueObject();

        String uuidField = reader.readString("uuidField");
        assert uuidField != null;
        UUID uuid = UUID.fromString(uuidField);
        valueObject.setUuid(uuid);
        return valueObject;
    }

    @Override
    public void write(CompactWriter writer, UUIDValueObject uuidValueObject) {
        writer.writeString("uuidField", uuidValueObject.getUuid().toString());
    }


    @Override
    public String getTypeName() {
        return "UUIDValueObject";
    }


    @Override
    public Class<UUIDValueObject> getCompactClass() {
        return UUIDValueObject.class;
    }
}