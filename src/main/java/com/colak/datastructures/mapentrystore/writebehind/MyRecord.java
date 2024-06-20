package com.colak.datastructures.mapentrystore.writebehind;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Getter
@RequiredArgsConstructor
@ToString
class MyRecord implements Serializable {

    private final String value;

}
