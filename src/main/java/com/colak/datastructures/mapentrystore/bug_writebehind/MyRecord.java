package com.colak.datastructures.mapentrystore.bug_writebehind;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@RequiredArgsConstructor
@ToString
class MyRecord implements Serializable {

    private final String value;

}
