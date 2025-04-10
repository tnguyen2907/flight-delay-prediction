from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import Evaluator
from pyspark.ml.param.shared import HasLabelCol, HasPredictionCol
import pandas as pd

def evaluate(transformed_df, print_metrics=False):
    pred_label_df = transformed_df.select("prediction", "DepartureDelayed").rdd.map(lambda row: (float(row[0]), float(row[1])))
    metrics = MulticlassMetrics(pred_label_df)
    confusion_matrix = metrics.confusionMatrix().toArray().astype(int)
    labels = [0.0, 1.0]

    cm_df = pd.DataFrame(
        confusion_matrix,
        index=[f"Actual {int(label)}" for label in labels],
        columns=[f"Predicted {int(label)}" for label in labels]
    )
    if print_metrics:
        print("Confusion Matrix:")
        print(cm_df)
    
    desired_label = 1.0
    
    accuracy = metrics.accuracy
    precision = metrics.precision(desired_label)
    recall = metrics.recall(desired_label)
    f2_score = metrics.fMeasure(desired_label, beta=2.0)

    prauc_evaluator = BinaryClassificationEvaluator(
        labelCol="DepartureDelayed", metricName="areaUnderPR"
    )
    prauc = prauc_evaluator.evaluate(transformed_df)

    if print_metrics:
        print(f"Accuracy: {accuracy}")
        print(f"Precision (Label {desired_label}): {precision}")
        print(f"Recall (Label {desired_label}): {recall}")
        print(f"F2-Score (Label {desired_label}) {f2_score}")
        print(f"Area Under Curve - Presicion-Recall : {prauc}")
    
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f2": f2_score,
        "prauc": prauc
    }
    
# class FBetaPosEvaluator(Evaluator, HasLabelCol, HasPredictionCol):
#     def __init__(self, labelCol="label", predictionCol="prediction", beta=1.0):
#         super(FBetaPosEvaluator, self).__init__()
#         self._set(
#             labelCol=labelCol,
#             predictionCol=predictionCol,
#         )
#         self.beta = beta
        
#     def _evaluate(self, dataset):
#         label_col = self.getLabelCol()
#         prediction_col = self.getPredictionCol()

#         pred_label_rdd = dataset.select(prediction_col, label_col).rdd.map(lambda row: (float(row[0]), float(row[1])))

#         metrics = MulticlassMetrics(pred_label_rdd)

#         f_beta_pos_score = metrics.fMeasure(1.0, beta=self.beta)
#         return f_beta_pos_score
        
    