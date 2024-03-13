package com.colak.serialization.compact.serializer_value_with_uuid;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
class UUIDValueObject {

    private UUID uuid;

    public static UUIDValueObject createNew () {
        UUIDValueObject valueObject = new UUIDValueObject();
        valueObject.setUuid(UUID.randomUUID());
        return valueObject;
    }
}
