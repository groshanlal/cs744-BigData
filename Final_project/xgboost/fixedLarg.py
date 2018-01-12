#!/usr/bin/python
import numpy as np
import xgboost as xgb
import FixedPoint
###
# advanced: customized loss function
#
print('start running example to use low precision objective function')

dtrain = xgb.DMatrix('agaricus.txt.train')
dtest = xgb.DMatrix('agaricus.txt.test')


# note: for customized objective function, we leave objective as default
# note: what we are getting is margin value in prediction

param = {'max_depth': 2, 'eta': 1, 'silent': 1}
watchlist = [(dtest, 'eval'), (dtrain, 'train')]
num_round = 100

# floating point data
f_X = X.applymap(
    lambda v: FixedPoint.FXnum(
            v,FixedPoint.FXfamily(n_bits=input_prec[0], n_intbits=input_prec[1])
    )
)
f_W = W.applymap(
    lambda v: FixedPoint.FXnum(
            v,FixedPoint.FXfamily(n_bits=inter_prec[0], n_intbits=inter_prec[1])
    )
)
f_y = y.applymap(
    lambda v: FixedPoint.FXnum(
            v,FixedPoint.FXfamily(n_bits=output_prec[0], n_intbits=output_prec[1])
    )
)

def lowprecisionObj():

def evalerror(preds, ):