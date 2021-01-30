package net.acilab.stream.utils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ApplicationConstants {

  public static final String APPLICATION_NAME = "DataPineline.Stream";
  public static final List<Integer> EVENT_FILE_INDEX_RANGE = IntStream.rangeClosed(0, 9).boxed()
      .collect(Collectors.toList());
}
