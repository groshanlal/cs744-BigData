./software/apache-storm-1.0.2/bin/storm nimbus &

ssh vm-32-2 "./software/apache-storm-1.0.2/bin/storm supervisor" &
ssh vm-32-3 "./software/apache-storm-1.0.2/bin/storm supervisor" &
ssh vm-32-4 "./software/apache-storm-1.0.2/bin/storm supervisor" &
ssh vm-32-5 "./software/apache-storm-1.0.2/bin/storm supervisor" &
