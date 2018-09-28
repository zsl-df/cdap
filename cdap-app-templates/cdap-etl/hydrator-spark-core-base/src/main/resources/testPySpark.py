from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

from cdap.pyspark import SparkExecutionContext
from pyspark.rdd import RDD  

from PySparkCompute import PySparkCompute

sc = SparkContext()
#sql = SQLContext(sc)

sec = SparkExecutionContext()
metrics = sec.getMetrics()

print("-----------------------INSIDE PYTASK------------------")

#sc._gateway.restart_callback_server()

rc = sec._runtimeContext.getSparkRuntimeContext()

#scl = sc._jvm.SparkClassLoader(rc)

scl = sc._jvm.SparkClassLoader.findFromContext()
#bspd = sc._jvm.co.cask.cdap.etl.spark.batch.BatchSparkPipelineDriver()
#scl = sc._jvm.SparkClassLoader.create()

print "scl==>"
print scl.getClass().getClassLoader().toString()

ssec = scl.getSparkExecutionContext(sc._jvm.java.lang.Boolean("false"))
serEC = sc._jvm.co.cask.cdap.app.runtime.spark.SerializableSparkExecutionContext(ssec)

print("--------------------------------")

bspd = scl.loadClass("co.cask.cdap.etl.spark.batch.BatchSparkPipelineDriver").newInstance()
#bspd = sc._jvm.co.cask.cdap.etl.spark.batch.BatchSparkPipelineDriver()

print "bspd==>"
print bspd.getClass().getClassLoader().toString()

bspd.setPySparkCompute("test", PySparkCompute("asd"))

#djsec = bspd.getClass().getClassLoader().loadClass("co.cask.cdap.app.runtime.spark.DefaultJavaSparkExecutionContext").getConstructors()[0].newInstance(serEC)
#print djsec.getClass().getClassLoader()

#bspd.run(djsec)
bspd.run(scl.createJavaExecutionContext(serEC))

print("---------------DONE------------------")
