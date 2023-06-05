import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utilities import SEED
# import any other dependencies you want, but make sure only to use the ones
# availiable on AWS EMR

# ---------------- choose input format, dataframe or rdd ----------------------
INPUT_FORMAT = 'dataframe'  # change to 'rdd' if you wish to use rdd inputs
# -----------------------------------------------------------------------------
if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.tree import DecisionTree
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import DenseVector
    from pyspark.mllib.evaluation import RegressionMetrics


# ---------- Begin definition of helper functions, if you need any ------------

# def task_1_helper():
#   pass

# -----------------------------------------------------------------------------


def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    review_transform = review_data.groupBy('asin').agg(
        F.avg('overall').alias('meanRating'), F.count('overall').alias('countRating'))
    transform = product_data[['asin']].join(review_transform, 'asin', "left")
    stats = transform.select(F.avg('meanRating'),
                             F.variance('meanRating'),
                             F.count(
                                 F.when(F.col('meanRating').isNull(), 'meanRating')),
                             F.avg('countRating'),
                             F.variance('countRating'),
                             F.count(
                                 F.when(F.col('countRating').isNull(), 'countRating'))
                             ).head()
    count_total = transform.count()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmaticly. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated with
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': None,
        'mean_meanRating': None,
        'variance_meanRating': None,
        'numNulls_meanRating': None,
        'mean_countRating': None,
        'variance_countRating': None,
        'numNulls_countRating': None
    }
    # Modify res:

    res['count_total'] = count_total
    res['mean_meanRating'] = stats[0]
    res['variance_meanRating'] = stats[1]
    res['numNulls_meanRating'] = stats[2]
    res['mean_countRating'] = stats[3]
    res['variance_countRating'] = stats[4]
    res['numNulls_countRating'] = stats[5]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    # -------------------------------------------------------------------------


def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    transform = product_data.withColumn("category", F.when((F.size(F.col("categories")) > 0) & (
        F.col("categories")[0][0] != ""), F.col("categories")[0][0]).otherwise(None))
    udf_transform = F.udf(lambda x: x[0] if x else None)
    transform = transform.withColumn(
        "bestSalesCategory", udf_transform(F.map_keys("salesRank")))
    transform = transform.withColumn(
        "bestSalesRank", udf_transform(F.map_values("salesRank")))

    stats = transform.select(F.count('asin'),
                             F.avg('bestSalesRank'),
                             F.variance('bestSalesRank'),
                             F.count(
                                 F.when(F.col('category').isNull(), 'category')),
                             F.countDistinct('category'),
                             F.count(
                                 F.when(F.col('bestSalesCategory').isNull(), 'bestSalesCategory')),
                             F.countDistinct('bestSalesCategory')
                             ).head()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_bestSalesRank': None,
        'variance_bestSalesRank': None,
        'numNulls_category': None,
        'countDistinct_category': None,
        'numNulls_bestSalesCategory': None,
        'countDistinct_bestSalesCategory': None
    }
    # Modify res:
    res['count_total'] = stats[0]
    res['mean_bestSalesRank'] = stats[1]
    res['variance_bestSalesRank'] = stats[2]
    res['numNulls_category'] = stats[3]
    res['countDistinct_category'] = stats[4]
    res['numNulls_bestSalesCategory'] = stats[5]
    res['countDistinct_bestSalesCategory'] = stats[6]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res
    # -------------------------------------------------------------------------


def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    product_data = product_data.withColumn(
        'also_viewed', F.col(related_column)['also_viewed'])

    count_data = product_data.select('asin', 'also_viewed', F.when(F.col('also_viewed').isNull(
    ), None).otherwise(F.size(F.col('also_viewed'))).alias('countAlsoViewed'))

    also_view_data = product_data.select('asin', F.explode('also_viewed'))
    also_view_data = also_view_data.withColumnRenamed('col', 'also_viewed')
    price = product_data.select('asin', 'price')
    price = price.withColumnRenamed('asin', "asin_price")

    viewed_price = also_view_data.join(
        price, also_view_data['also_viewed'] == price["asin_price"], "left")
    mean_price_data = viewed_price.groupBy('asin').agg(
        F.avg('price').alias('meanPriceAlsoViewed'))

    count_mean = count_data.join(
        mean_price_data, count_data['asin'] == mean_price_data['asin'], "left")
    count_total = count_mean.count()
    stats = count_mean.select(F.avg('meanPriceAlsoViewed'),
                              F.variance('meanPriceAlsoViewed'),
                              F.count(
                                  F.when(F.col('meanPriceAlsoViewed').isNull(), 'meanPriceAlsoViewed')),
                              F.avg('countAlsoViewed'),
                              F.variance('countAlsoViewed'),
                              F.count(
                                  F.when(F.col('countAlsoViewed').isNull(), 'countAlsoViewed'))
                              ).head()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanPriceAlsoViewed'] = stats[0]
    res['variance_meanPriceAlsoViewed'] = stats[1]
    res['numNulls_meanPriceAlsoViewed'] = stats[2]
    res['mean_countAlsoViewed'] = stats[3]
    res['variance_countAlsoViewed'] = stats[4]
    res['numNulls_countAlsoViewed'] = stats[5]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return res
    # -------------------------------------------------------------------------


def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    transform = product_data.withColumn(
        "price", product_data["price"].cast("float"))
    calculation = product_data.select(
        F.avg('price'), F.percentile_approx('price', 0.5)).head()

    transform = transform.withColumn("meanImputedPrice", F.when(F.col("price").isNull(), calculation[0]).otherwise(F.col("price")))\
        .withColumn("medianImputedPrice", F.when(F.col("price").isNull(), calculation[1]).otherwise(F.col("price")))
    transform = transform.withColumn('unknownImputedTitle', F.when(F.col(
        "title").isNull() | (F.col("title") == ""), 'unknown').otherwise(F.col("title")))

    count_total = transform.count()
    stats = transform.select(F.avg('meanImputedPrice'),
                             F.variance('meanImputedPrice'),
                             F.count(
                                 F.when(F.col('meanImputedPrice').isNull(), 'meanImputedPrice')),
                             F.avg('medianImputedPrice'),
                             F.variance('medianImputedPrice'),
                             F.count(
                                 F.when(F.col('medianImputedPrice').isNull(), 'medianImputedPrice')),
                             F.count(
                                 F.when(F.col('unknownImputedTitle') == 'unknown', 1))
                             ).head()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanImputedPrice'] = stats[0]
    res['variance_meanImputedPrice'] = stats[1]
    res['numNulls_meanImputedPrice'] = stats[2]
    res['mean_medianImputedPrice'] = stats[3]
    res['variance_medianImputedPrice'] = stats[4]
    res['numNulls_medianImputedPrice'] = stats[5]
    res['numUnknowns_unknownImputedTitle'] = stats[6]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------


def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    output = product_processed_data[[title_column]].withColumn(
        'titleArray', F.split(F.lower('title'), ' '))
    vector = M.feature.Word2Vec(minCount=100, vectorSize=16, seed=SEED, numPartitions=4,
                                inputCol='titleArray', outputCol='titleVector')
    model = vector.fit(output)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res['count_total'] = output.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res
    # -------------------------------------------------------------------------


def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    si = M.feature.StringIndexer(
        inputCol='category', outputCol='categoryIndex', stringOrderType="frequencyDesc")
    si_model = si.fit(product_processed_data)
    index = si_model.transform(product_processed_data)

    ohe = M.feature.OneHotEncoder(
        inputCol=categoryIndex_column, outputCol=categoryOneHot_column, dropLast=False)
    ohe_model = ohe.fit(index)
    idx_oh = ohe_model.transform(index)

    pca = M.feature.PCA(k=15, inputCol=categoryOneHot_column,
                        outputCol=categoryPCA_column)
    pac_model = pca.fit(idx_oh)
    idx_oh_pca = pac_model.transform(idx_oh)

    stats = idx_oh_pca.select(M.stat.Summarizer.mean(
        F.col('categoryOneHot')), M.stat.Summarizer.mean(F.col('categoryPCA'))).head()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:
    res['count_total'] = idx_oh_pca.count()
    res['meanVector_categoryOneHot'] = stats[0]
    res['meanVector_categoryPCA'] = stats[1]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res
    # -------------------------------------------------------------------------


def task_7(data_io, train_data, test_data):

    # ---------------------- Your implementation begins------------------------
    decisionTree = M.regression.DecisionTreeRegressor(
        maxDepth=5, labelCol='overall')
    dt_model = decisionTree.fit(train_data)
    test = dt_model.transform(test_data)
    eva = M.evaluation.RegressionEvaluator(
        labelCol='overall', metricName='rmse')
    RMSE = eva.evaluate(test)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None
    }
    # Modify res:
    res['test_rmse'] = RMSE

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res
    # -------------------------------------------------------------------------


def task_8(data_io, train_data, test_data):

    # ---------------------- Your implementation begins------------------------
    train_data, val_data = train_data.randomSplit([0.75, 0.25], seed=42)
    evaluator = M.evaluation.RegressionEvaluator(
        labelCol='overall', metricName="rmse")
    rmse = []

    dt = M.regression.DecisionTreeRegressor(maxDepth=5, labelCol='overall')
    model = dt.fit(train_data)
    predictions = model.transform(val_data)
    val_rmse = evaluator.evaluate(predictions)
    rmse.append(val_rmse)
    best_rmse = val_rmse
    best_model = model

    for max_depth in [7, 9, 12]:
        dt = M.regression.DecisionTreeRegressor(
            maxDepth=max_depth, labelCol='overall')
        model = dt.fit(train_data)
        predictions = model.transform(val_data)
        val_rmse = evaluator.evaluate(predictions)
        rmse.append(val_rmse)
        if val_rmse < best_rmse:
            best_rmse = val_rmse
            best_model = model

    testData = best_model.transform(test_data)
    test_rmse_value = evaluator.evaluate(testData)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:
    res['test_rmse'] = test_rmse_value
    res['valid_rmse_depth_5'] = rmse[0]
    res['valid_rmse_depth_7'] = rmse[1]
    res['valid_rmse_depth_9'] = rmse[2]
    res['valid_rmse_depth_12'] = rmse[3]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
    # -------------------------------------------------------------------------
