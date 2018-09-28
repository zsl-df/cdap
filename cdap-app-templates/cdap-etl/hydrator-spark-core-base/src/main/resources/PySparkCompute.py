import traceback

class PySparkCompute(object):
    def __init__(self, x):
        self.x = x
    
    def transform(self, context, input):
        try:
            print("------------REACHED IN PYCOMPUTE----------")
            print input
        except:
            print '>>> end of traceback <<<'
        return input

    def toString(self):
        return "PySparkCompute",self.x

    class Java:
        implements = ["co.cask.cdap.app.runtime.spark.etl.pyspark.PySparkComputeInterface"]
