import tensorflow as tf

# number of features in the criteo dataset after one-hot encoding
num_features = 100#33762578


# Here, we will show how to include reader operators in the TensorFlow graph.
# These operators take as input list of filenames from which they read data.
# On every invocation of the operator, some records are read and passed to the
# downstream vertices as Tensors
s_batch = 2

g = tf.Graph()

with g.as_default():

    # We first define a filename queue comprising 5 files.
    filename_queue = tf.train.string_input_producer([
        "/home/ubuntu/criteo-tfr-tiny/tfrecords00",
        "/home/ubuntu/criteo-tfr-tiny/tfrecords01",
        "/home/ubuntu/criteo-tfr-tiny/tfrecords02",
        "/home/ubuntu/criteo-tfr-tiny/tfrecords03",
        "/home/ubuntu/criteo-tfr-tiny/tfrecords04",
    ], num_epochs=None)


    # TFRecordReader creates an operator in the graph that reads data from queue
    reader = tf.TFRecordReader()

    # Include a read operator with the filenae queue to use. The output is a string
    # Tensor called serialized_example
    _, serialized_example = reader.read(filename_queue)


    # The string tensors is essentially a Protobuf serialized string. With the
    # following fields: label, index, value. We provide the protobuf fields we are
    # interested in to parse the data. Note, feature here is a dict of tensors
    features = tf.parse_single_example(serialized_example,
                                       features={
                                        'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                        'index' : tf.VarLenFeature(dtype=tf.int64),
                                        'value' : tf.VarLenFeature(dtype=tf.float32),
                                       }
                                      )

    label = features['label']
    index = features['index']
    value = features['value']

    # These print statements are there for you see the type of the following
    # variables
    print label
    print index
    print value

    # since we parsed a VarLenFeatures, they are returned as SparseTensors.
    # To run operations on then, we first convert them to dense Tensors as below.
    dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                   [33762578,],
    #                               tf.constant([33762578, 1], dtype=tf.int64),
                                   tf.sparse_tensor_to_dense(value))



    # min_after_dequeue defines how big a buffer we will randomly sample
    #   from -- bigger means better shuffling but slower start up and more
    #   memory used.
    # capacity must be larger than min_after_dequeue and the amount larger
    #   determines the maximum we will prefetch.  Recommendation:
    #   min_after_dequeue + (num_threads + a small safety margin) * batch_size
    min_after_dequeue = 10000
    capacity = min_after_dequeue + 3 * s_batch
    example_batch, label_batch = tf.train.shuffle_batch(
      [dense_feature[0:num_features], label], batch_size=s_batch, capacity=capacity,
      min_after_dequeue=min_after_dequeue)



    # as usual we create a session.
    sess = tf.Session()
    sess.run(tf.initialize_all_variables())

    # this is new command and is used to initialize the queue based readers.
    # Effectively, it spins up separate threads to read from the files
    tf.train.start_queue_runners(sess=sess)

    for i in range(0, 20):
        # every time we call run, a new data point is read from the files
        output, lbl =  sess.run([example_batch,label_batch])
        print output.shape
        print sum(output)
        print lbl