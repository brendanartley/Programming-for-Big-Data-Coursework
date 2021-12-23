## Answers

1. What is your best guess for the slope and intercept of the streaming points being produced?

    After around 200 batches of streaming data from the Kaftka node using xy-100, the slope and intercept seemed to converge with a slope of ~6.1125 and an Intercept of ~ -9.6304. These values we aggregated based on all the batches setting "complete" as the output mode.

2. Is your streaming program's estimate of the slope and intercept getting better as the program runs? (That is: is the program aggregating all of the data from the start of time, or only those that have arrived since the last output?)

    Yes, by using "complete" as the output mode I found that the program estimate was getting better over time. This is because the aggregation is occurring on all values from the start of the program rather than aggregating only the new values that are streamed in. 

3. In the colour classification question, what were your validation scores for the RGB and LAB pipelines?

    In the colour classification, I tried a few different model structures but the model that performed the best was the Multi-Layer-Perceptron classifier structure suggested in the assignment with different hyperparameter selections for the RGB and LAB inputs. More specifically, for the RGB inputs, the model that worked best had 1 hidden layer with 180 nodes, and the maxIter parameter was set to 100. For the LAB inputs, the model that worked best had 1 hidden layer with 100 hidden nodes, and the maxIter parameter was set to 150.

    I used the Multi-Layer-Perceptron model with both the RGB and the LAB pipelines and got decent results when compared to other models I tested. The validation score for the RGB pipeline was ~0.69 and for the LAB pipeline, we got a score of ~0.76. This makes sense as the LAB color range is more aligned with human perception of color.

    Note that I also changed the size of the validation and training set from [0.75, 0.25] to [0.80, 0.20] as we have 5000 samples, and increasing the size of the training data will not create a poor representation of the data in the validation set. I have included the complete validation scores below. Note that the default evaluation metric is F1 score.

    +--------------+-------------------+
    |Color Space   |F1-Score           |
    +--------------+-------------------+
    |RGB           |0.69320            |
    |LAB           |0.76659            |
    +--------------+-------------------+

4. When predicting the tmax values, did you over-fit the training data (and for which training/validation sets)?

    When predicting the tmax values, I was using a GBTRegressor with the max_iter parameter set to 10. Using the root mean squared error metric to evaluate the model performance on the training and validation datasets the model was overfitting in some cases and not overfitting in others. 
    
    NOTE: I included a seed in my .py files to make these results reproducible.

    -- Without "tomorrow's temperature" -- 

    When not including "tomorrow's temperature" as a feature in the dataset, the RMSE on the training set was ~4.34 and the RMSE on the validation set was ~4.61. The training error is slightly lower than the validation error but that that is expected when training an ML model. As the training error is slightly lower than the validation error, you could say that the model was marginally overfitting the training set. That being said, there is not a large enough difference between the training and validation errors to say that the model is overfitting by a large amount.

    I have included the complete RMSE values for the training and validation sets below.

    +--------------+------------------+-----------------------+
    |Set           |RMSE              |R^2                    |
    +--------------+------------------+-----------------------+
    |Train         |4.34241           |0.87876                |
    |Validation    |4.61988           |0.84841                |
    +--------------+------------------+-----------------------+

    -- With "tomorrow's temperature" -- 

    On the other hand, when we added tomorrow's temperature as a feature using the same model structure, the model was overfitting. The root mean squared error on the training set was ~3.31 and the root mean squared error on the validation set was ~4.03. As the training error is significantly lower than the validation error we can say that the model is overfitting and we should consider reducing model complexity, finding more data, and/or testing other model types in order to combat overfitting.

    I have included the complete RMSE values for the training and validation sets below.

    +--------------+------------------+-----------------------+
    |Set           |RMSE              |R^2                    |
    +--------------+------------------+-----------------------+
    |Train         |3.31997           |0.93130                |
    |Validation    |4.03211           |0.86491                |
    +--------------+------------------+-----------------------+

    -- with tmax-3 -- 

    I went a step further and trained the same model with different hyperparameter values on tmax-3. I increased the max_iter parameter from 10 to 50 and found that this produced a very generalizable model. The validation and training errors were consistent and the model was not overfitting or underfitting during training. I have included the training and validation errors when training on tmax-3 in the table below.

    +------------------------------+------------------+-----------------+
    |Set                           |Train RMSE        |Validation RMSE  |
    +------------------------------+------------------+-----------------+
    |tmax-3                        |6.25671           |6.24202          |
    |tmax-3-with-yesterday-tmax    |3.77259           |3.72283          |
    +------------------------------+------------------+-----------------+

5. What were your testing scores for your model with and without the “yesterday's temperature” feature?

    I first trained two models locally using the tmax-1 dataset on my local machine and got some interesting results. The RSME value when "yesterday_tmax" was not included was ~10.64 on the test dataset, and when we did include "yesterday_tmax" the RMSE error on the test dataset dropped to ~4.76.

    Next, I decided to go bigger and train the two models on the cluster with tmax-3. Given that we would be training on more data, It would make sense that we would end up with a more accurate model compared to training solely on tmax-1. Although having the max iterations parameter set to 10 when training on tmax-1 worked well, when training on tmax-3 this resulted in a model that was severely underfitting. I increased the max iterations parameter to 50 when training on tmax-3 and got much better error values (see more about this in question 4 answer). 

    I have included the results of each model testing on tmax-1 and on tmax-3 in the dataframe below. 

    +-------------------------+-----------------------+
    |Trained On               |Test RMSE              |
    +-------------------------+-----------------------+
    |tmax-1                   |10.64024               |
    |tmax-1-with-yesterday    |4.76117                |
    |tmax-3                   |6.356194               |
    |tmax-3-with-yesterday    |3.82011                |
    +-------------------------+-----------------------+

6. If you're using a tree-based model, you'll find a .featureImportances property that describes the relative importance of each feature (code commented out in weather_test.py; if not, skip this question). Have a look with and without the “yesterday's temperature” feature: do the results make sense and suggest that your model is making decisions reasonably? With “yesterday's temperature”, is it just predicting “same as yesterday”?

    Without the yeaterday_tmax feature, we can see that latitude and the day of the year account for 37% and 41% of the "feature importances" whereas longitude accounts for 12% and elevation accounts for 9%. On the other hand, when we include yesterdays_tmax as a feature these percentages change significantly. Yesterday's max temperature accounts for 86% of the feature importance, followed by day of the year with 7%, latitude with 3%, elevation with 1%, and longitude with 1% as well. 

    Technically the model is still making reasonable decisions because it has determined that yesterday's temperature is highly relevant in predicting today's temperature compared with the other features. In other words, our model is more accurate with this feature than without. That being said, it is making its decision almost solely based on yesterday's temperature and that is not always the best thing. In practice, it would be beneficial to see if we can feature engineer other features that could contribute some more relevant information to the model so that the model is not making predictions based on one feature.

    I have included the feature_importances in the two dataframes below.

    -- without yeaterday_tmax -- 

    +-------------------------+-----------------------+
    |Set                      |Test RMSE              |
    +-------------------------+-----------------------+
    |latitude                 |37.5%                  |
    |longitude                |1.2%                   |
    |elevation                |9.0%                   |
    |dayofyear                |41.0%                  |
    +-------------------------+-----------------------+

    -- with yeaterday_tmax -- 

    +-------------------------+-----------------------+
    |Set                      |Test RMSE              |
    +-------------------------+-----------------------+
    |latitude                 |3.2%                   |
    |longitude                |1.2%                   |
    |elevation                |1.8%                   |
    |dayofyear                |7.0%                   |
    |yesterday_tmax           |86.6%                  |
    +-------------------------+-----------------------+