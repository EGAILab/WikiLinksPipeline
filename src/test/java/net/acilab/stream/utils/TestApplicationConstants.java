package net.acilab.stream.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestApplicationConstants {

    public static final String APPLICATION_NAME = "DataPineline.Stream";

    public static final List<Integer> EVENT_FILE_INDEX_RANGE = IntStream.rangeClosed(0, 9).boxed()
            .collect(Collectors.toList());

    public static final List<String> EVENT_FILE_LIST = Collections.unmodifiableList(Arrays.asList(
            new String[] { "F:\\Data\\wiki-links_google\\data-0-of-9", "F:\\Data\\wiki-links_google\\data-1-of-9",
                    "F:\\Data\\wiki-links_google\\data-2-of-9", "F:\\Data\\wiki-links_google\\data-3-of-9",
                    "F:\\Data\\wiki-links_google\\data-4-of-9", "F:\\Data\\wiki-links_google\\data-5-of-9",
                    "F:\\Data\\wiki-links_google\\data-6-of-9", "F:\\Data\\wiki-links_google\\data-7-of-9",
                    "F:\\Data\\wiki-links_google\\data-8-of-9", "F:\\Data\\wiki-links_google\\data-9-of-9" }));

    public static final List<String> EVENT_POINTER_FILE_LIST = Collections
            .unmodifiableList(Arrays.asList(new String[] { "F:\\Data\\wiki-links_google\\data-0-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-1-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-2-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-3-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-4-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-5-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-6-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-7-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-8-of-9_pointer.txt",
                    "F:\\Data\\wiki-links_google\\data-9-of-9_pointer.txt" }));

}
