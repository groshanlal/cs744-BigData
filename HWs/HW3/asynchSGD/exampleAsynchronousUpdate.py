import tensorflow as tf
import os

# number of features in the criteo dataset after one-hot encoding
num_features = 33762578 # DO NOT CHANGE THIS VALUE

s_batch = 100
eta = 10
train_test_ratio = 1000
total_trains = 100001
iterations = total_trains/(5*s_batch)


s_test = 20;
total_tests = 2000;

file_dict = {0:["00","01","02","03","04"],
             1:["05","06","07","08","09"],
             2:["10","11","12","13","14"],
             3:["15","16","17","18","19"],
             4:["20","21"],
            -1:["22"]}


tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-48-1
    with tf.device("/job:worker/task:0"):
        w_asynch = tf.Variable(tf.ones([10, 1]), name="model")

    # creating only reader and gradient computation operator
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    with tf.device("/job:worker/task:%d" % FLAGS.task_index):
        reader_asynch = tf.ones([10, 1], name="operator_%d" % FLAGS.task_index)
        # not the gradient compuation here is a random operation. You need
        # to use the right way (as described in assignment 3 desc).
        # we use this specific example to show that gradient computation
        # requires use of the model
        local_gradient_asynch = tf.mul(reader_asynch, tf.matmul(tf.transpose(w_asynch), reader_asynch))

    with tf.device("/job:worker/task:0"):
        assign_op_asynch = w.assign_add(tf.mul(local_gradient_asynch, 0.001))


    with tf.Session("grpc://vm-32-%d:2222" % (FLAGS.task_index+1)) as sess_asynch:

        # only one client initializes the variable
        if FLAGS.task_index == 0:
            sess_asynch.run(tf.initialize_all_variables())
        for i in range(0, 1000):
            sess.run(assign_op_asynch)
            print w_asynch.eval()
        sess.close()
