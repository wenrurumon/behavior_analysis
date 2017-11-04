start_time = time.time()
#[Info] error computing cost time 3627s

# Evaluate model on test instances and compute test error
predictions             = model.predict(testData.map(lambda x: x.features))
labels_and_predictions  = testData.map(lambda lp: lp.label).zip(predictions)


 # Evaluate model on test instances and compute test error
#predictions = model.predict(testData.map(lambda x: x.features))
#labels_and_predictions = testData.map(lambda lp: lp.label).zip(predictions)

labels_and_predictions.cache()
testData.cache()

testMSE = labels_and_predictions.map(lambda lp: (lp[0] - lp[1]) * (lp[0] - lp[1])).sum() / float(testData.count())
print('Test Mean Squared Error = ' + str(testMSE))


err_matrix = {}

#err_matrix['total'] = testData.count()

err_matrix['tp'] = labels_and_predictions.filter(lambda lp: (lp[0] >= 0.5 and lp[1] >= 0.5 )).count()
err_matrix['tn'] = labels_and_predictions.filter(lambda lp: (lp[0] <  0.5 and lp[1] < 0.5)).count()

err_matrix['fp'] = labels_and_predictions.filter(lambda lp: (lp[0] <  0.5 and lp[1] >= 0.5 )).count()
err_matrix['fn'] = labels_and_predictions.filter(lambda lp: (lp[0] >= 0.5 and lp[1] <  0.5 )).count()

err_matrix['tpr'] = 1.0*err_matrix['tp']/(err_matrix['tp'] + err_matrix['fn'])
err_matrix['acc'] = 1.0*(err_matrix['tp'] + err_matrix['tn']) / (err_matrix['tp'] + err_matrix['tn'] + err_matrix['fp'] + err_matrix['fn'])

for k,v in err_matrix.items():
    print(k,v)

print("[Info] error computing cost time %ss" % int(time.time() - start_time)) 
