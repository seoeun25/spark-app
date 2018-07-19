package com.lezhin.wasp.util;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author seoeun
 * @since 2018.07.19
 */
public class UtilsTest {

    @Test
    public void testComibination() {

        List<Long> contentIds = ImmutableList.of(1L, 22L, 33L, 44L, 55L );

        List<List<Long>> combinations = Utils.combinator(contentIds);

        for(List<Long> pair: combinations) {
            assertEquals(2, pair.size());
            System.out.println(pair);
        }
        assertEquals(20, combinations.size());

        assertEquals(0, Utils.combinator(ImmutableList.of(1L)).size());

        List<List<Long>> combinations2 = Utils.combinator(ImmutableList.of(1L, 2L));
        assertEquals(2, combinations2.size());
        List<Long> comb = combinations2.get(0);
        System.out.println(comb);
        assertEquals(1L, comb.get(0).longValue());
        assertEquals(2L, comb.get(1).longValue());
        List<Long> comb2 = combinations2.get(1);
        System.out.println(comb2);
        assertEquals(2L, comb2.get(0).longValue());
        assertEquals(1L, comb2.get(1).longValue());


    }
}
