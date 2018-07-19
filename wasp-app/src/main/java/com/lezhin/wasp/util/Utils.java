package com.lezhin.wasp.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author seoeun
 * @since 2018.07.19
 */
public class Utils {

    /**
     * Return possible combination of 2 element from the given list.
     * @param list
     * @param <E>
     * @return
     */
    public static <E> List<List<E>> combinator(List<E> list) {
        List<List<E>> combinations = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {

            E a = list.get(i);
            for (int j = 0; j <list.size(); j++) {
                if (i != j) {
                    List<E> comb = new ArrayList<>();
                    comb.add(a);
                    comb.add(list.get(j));
                    combinations.add(comb);
                }
            }

        }
        return combinations;
    }
}
