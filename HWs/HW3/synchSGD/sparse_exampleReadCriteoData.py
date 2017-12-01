import tensorflow as tf

# number of features in the criteo dataset after one-hot encoding
num_features = 33762578
s_batch = 1
iterations = 1;


# Here, we will show how to include reader operators in the TensorFlow graph.
# These operators take as input list of filenames from which they read data.
# On every invocation of the operator, some records are read and passed to the
# downstream vertices as Tensors


file_dict = {0:["00","01","02","03","04"],
             1:["05","06","07","08","09"],
             2:["10","11","12","13","14"],
             3:["15","16","17","18","19"],
             4:["20","21"],
            -1:["22"]}



g = tf.Graph()

with g.as_default():

    def get_datapoint_iter(file_idx=[]):
        fileNames = map(lambda s: "/home/ubuntu/criteo-tfr-tiny/tfrecords"+s,file_idx)
        # We first define a filename queue comprising 5 files.
        filename_queue = tf.train.string_input_producer(fileNames, num_epochs=None)


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
        # dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
        #                                [33762578,],
        # #                               tf.constant([33762578, 1], dtype=tf.int64),
        #                                tf.sparse_tensor_to_dense(value))


        label_flt = tf.cast(label, tf.float32)
        # min_after_dequeue defines how big a buffer we will randomly sample
        #   from -- bigger means better shuffling but slower start up and more
        #   memory used.
        # capacity must be larger than min_after_dequeue and the amount larger
        #   determines the maximum we will prefetch.  Recommendation:
        #   min_after_dequeue + (num_threads + a small safety margin) * batch_size
        min_after_dequeue = 10
        capacity = min_after_dequeue + 3 * s_batch
        index_batch, value_batch,label_batch = tf.train.shuffle_batch(
          [index, value, label_flt], batch_size=s_batch, capacity=capacity,
          min_after_dequeue=min_after_dequeue)

        return index_batch, value_batch,label_batch


    def calc_gradient(X,W,Y):
        error = tf.sigmoid(tf.mul(Y,tf.matmul(X,W)))
        print "error:",error.get_shape()
        error_m1 = error-1
        print "error_m1:",error_m1.get_shape()
        gradient = tf.matmul(tf.transpose(X),tf.mul(Y,error_m1))
        print "gradient:",gradient.get_shape()
        return tf.reduce_sum(gradient,1)

    def sparse_matmul(indices, values, B):
        pass


    w = tf.Variable(tf.ones([num_features, 1]), name="model")

    index_batch, value_batch,label_batch = get_datapoint_iter(file_dict[0] )

    # grad = calc_gradient(example_batch,w,label_batch)


    # as usual we create a session.
    sess = tf.Session()
    sess.run(tf.initialize_all_variables())

    # this is new command and is used to initialize the queue based readers.
    # Effectively, it spins up separate threads to read from the files
    tf.train.start_queue_runners(sess=sess)

    for i in range(iterations):
        # every time we call run, a new data point is read from the files
        idx, val, lbl =  sess.run([index_batch, value_batch,label_batch])
        print "idx:"idx
        print "valeus:",val
        # print sum(output)
        print "labels",lbl