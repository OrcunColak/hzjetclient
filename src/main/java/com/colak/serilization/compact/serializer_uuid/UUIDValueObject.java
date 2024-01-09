package com.colak.serilization.compact.serializer_uuid;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
class UUIDValueObject {

    UUID uuid;

    public static UUIDValueObject createNew () {
        UUIDValueObject valueObject = new UUIDValueObject();
        valueObject.setUuid(UUID.randomUUID());
        return valueObject;
    }
}
