import tensorflow as tf
import os

tf.logging.set_verbosity(tf.logging.DEBUG)

# number of features in the criteo dataset after one-hot encoding
num_features = 10
s_batch = 1
n_iter = 1

eta = 0.1


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
        # We first define a filename queue comprising 5 files.
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
        # print label
        # print index
        # print value

        # since we parsed a VarLenFeatures, they are returned as SparseTensors.
        # To run operations on then, we first convert them to dense Tensors as below.
        dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                       [33762578,],
        #                               tf.constant([33762578, 1], dtype=tf.int64),
                                       tf.sparse_tensor_to_dense(value))

        label_flt = tf.cast(label, tf.float32)
        # min_after_dequeue defines how big a buffer we will randomly sample
        #   from -- bigger means better shuffling but slower start up and more
        #   memory used.
        # capacity must be larger than min_after_dequeue and the amount larger
        #   determines the maximum we will prefetch.  Recommendation:
        #   min_after_dequeue + (num_threads + a small safety margin) * batch_size
        min_after_dequeue = 10
        capacity = min_after_dequeue + 3 * s_batch
        example_batch, label_batch = tf.train.shuffle_batch(
          [dense_feature[0:num_features], label_flt], batch_size=s_batch, capacity=capacity,
          min_after_dequeue=min_after_dequeue)



        return (example_batch,label_batch)
    ## END OF get_datapoint_iter

    def next_batch(id = 0):
        x_,y_ = get_datapoint_iter(file_dict[id])
        return x_,y_ 
        
    # END OF next_batch

    def calc_gradient(X,W,Y):
        error = tf.sigmoid(tf.mul(Y,tf.matmul(X,W)))
        error_m1 = error-1
        gradient = tf.matmul(tf.transpose(X),tf.mul(Y,error_m1))
        # generate the local sum of the batch
        return tf.reduce_sum(gradient,1)
    # END OF calc_gradient


    # creating a model variable on task 0. This is a process running on node vm-32-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.ones([num_features, 1]), name="model")


    # creating 5 reader operators to be placed on different operators
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    gradients = []
    for i in range(0, 5):
        with tf.device("/job:worker/task:%d" % i):
            # reader = tf.ones([num_features, 1], name="operator_%d" % i)
            X,Y = get_datapoint_iter(file_dict[i])#next_batch(i)

            print i,"> X:",X.get_shape()
            gradients.append(X)
            # temp = X#tf.reduce_sum(X,0)

            # # not the gradient compuation here is a random operation. You need
            # # to use the right way (as described in assignment 3 desc).
            # # we use this specific example to show that gradient computation
            # # requires use of the model
            # # local_gradient = tf.mul(reader, tf.matmul(tf.transpose(w), reader))
            # local_gradient = temp #calc_gradient(X,w,Y)
            # print i,"> local_gradient:",local_gradient.get_shape()
            # gradients.append(tf.mul(local_gradient, eta))
            # # gradients.append(tf.mul(reader, eta))


    # we create an operator to aggregate the local gradients
    # with tf.device("/job:worker/task:0"):
    #     aggregator = tf.add_n(gradients)
    #     agg_shape = tf.reshape(aggregator,[num_features, 1])
    #     print "agg_shape:",agg_shape.get_shape()
    #     assign_op = w.assign_add(agg_shape)


    with tf.Session("grpc://vm-32-1:2222") as sess:
        tf.train.SummaryWriter("%s/synchSGD" % (os.environ.get("TF_LOG_DIR")), sess.graph)
        sess.run(tf.initialize_all_variables())
        print "running"
        for i in range(n_iter):
            out = sess.run(gradients)
            print out
        print "END"
        sess.close()
