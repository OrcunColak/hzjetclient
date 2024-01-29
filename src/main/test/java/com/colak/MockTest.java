package com.colak;

import com.hazelcast.map.IMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MockTest {

    @Test
    void testMockIMap() {
        IMap<String, Integer> mockMap = Mockito.mock(IMap.class);
        Mockito.when(mockMap.get("key1")).thenReturn(1);
        Integer result = mockMap.get("key1");
        Assertions.assertEquals(1, result);
    }
}
