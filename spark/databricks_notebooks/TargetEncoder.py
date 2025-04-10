from pyspark.ml import Estimator, Model
from pyspark.ml.param.shared import Param, Params, TypeConverters, HasInputCol, HasOutputCol, HasInputCols, HasOutputCols, HasLabelCol, HasHandleInvalid
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as F

class HasGlobalMean(Params):
    global_mean = Param(Params._dummy(), "global_mean", "Global mean of the label", typeConverter=TypeConverters.toFloat)

    def __init__(self):
        super(HasGlobalMean, self).__init__()

    def setGlobalMean(self, value):
        return self._set(global_mean=value)

    def getGlobalMean(self):
        return self.getOrDefault(self.global_mean)

class HasEncodingMaps(Params):
    encoding_maps = Param(Params._dummy(), "encoding_maps", "Encoding maps for target encoding", typeConverter=TypeConverters.toList)

    def __init__(self):
        super(HasEncodingMaps, self).__init__()

    def setEncodingMaps(self, value):
        return self._set(encoding_maps=value)

    def getEncodingMaps(self):
        return self.getOrDefault(self.encoding_maps)

class TargetEncoder(Estimator, HasInputCol, HasOutputCol, HasInputCols, HasOutputCols, HasLabelCol, HasHandleInvalid, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        inputCols=None,
        outputCols=None,
        labelCol=None,
        handleInvalid="error"
    ):
        super(TargetEncoder, self).__init__()
        self._set(
            inputCol=inputCol,
            outputCol=outputCol,
            inputCols=inputCols,
            outputCols=outputCols,
            labelCol=labelCol,
            handleInvalid=handleInvalid
        )

    def _fit(self, dataset):
        inputCol = self.getInputCol()
        outputCol = self.getOutputCol()
        inputCols = self.getInputCols()
        outputCols = self.getOutputCols()
        labelCol = self.getLabelCol()

        if inputCol is not None:
            inputCols = [inputCol]
            outputCols = [outputCol]

        global_mean = dataset.agg(F.mean(labelCol)).collect()[0][0]
        encoding_maps = []
        for col in inputCols:
            rows = dataset \
                .groupBy(col) \
                .agg(F.mean(labelCol).alias(f"{col}Mean")) \
                .collect()
            encoding_lookup = [(row[col], row[f"{col}Mean"]) for row in rows]
            encoding_maps.append(encoding_lookup)
            
        return TargetEncoderModel(
            encoding_maps=encoding_maps,
            global_mean=global_mean,
            inputCols=inputCols,
            outputCols=outputCols,
            labelCol=labelCol,
            handleInvalid=self.getHandleInvalid()
        )
    
class TargetEncoderModel(Model, HasInputCols, HasOutputCols, HasLabelCol, HasHandleInvalid, HasEncodingMaps, HasGlobalMean, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(
        self,
        encoding_maps=None,
        global_mean=None,
        inputCols=None, 
        outputCols=None, 
        labelCol=None, 
        handleInvalid="error"
    ):
        super(TargetEncoderModel, self).__init__()
        self._set(
            encoding_maps=encoding_maps,
            global_mean=global_mean,
            inputCols=inputCols,
            outputCols=outputCols,
            labelCol=labelCol,
            handleInvalid=handleInvalid
        )

    def _transform(self, dataset):
        for inputCol, outputCol, encoding_map in zip(self.getInputCols(), self.getOutputCols(), self.getEncodingMaps()):
            encoding_map_df = dataset.sparkSession.createDataFrame(encoding_map, [inputCol, f"{inputCol}Mean"])
            dataset = dataset.join(F.broadcast(encoding_map_df), on=inputCol, how="left")

            if self.getHandleInvalid() == "keep":
                dataset = dataset.withColumn(
                    outputCol,
                    F.coalesce(
                        F.col(f"{inputCol}Mean"),
                        F.lit(self.getGlobalMean())
                    )
                )
            elif self.getHandleInvalid() == "skip":
                dataset = dataset \
                    .filter(F.col(f"{inputCol}Mean").isNotNull()) \
                    .withColumn(outputCol, F.col(f"{inputCol}Mean"))
            else:
                if dataset.filter(F.col(f"{inputCol}Mean").isNull()).limit(1).collect():
                    raise ValueError(f"TargetEncoder found invalid values in {inputCol}")

            dataset = dataset.drop(f"{inputCol}Mean")

        return dataset