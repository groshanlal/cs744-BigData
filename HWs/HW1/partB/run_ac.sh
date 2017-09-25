rm -r ac.jar anagramsorter_classes/*
hadoop fs -rm -r output_dir/output
hadoop fs -rm -r inter_dir/output

javac -classpath $(hadoop classpath) -d anagramsorter_classes AnagramSorter.java
jar -cvf ac.jar -C anagramsorter_classes/ .
hadoop jar ac.jar org.myorg.AnagramSorter input_dir/input.txt output_dir/output
