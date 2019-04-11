package cn.shicheng.day01

import org.apache.flink.api.scala._


object Demo1 {

  def main(args: Array[String]): Unit = {

    //get方法，会自动获取环境并且配置好相应的参数。

    //批处理，使用ExecutionEnvironment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val data: DataSet[String] = env.readTextFile("file:///C:\\Users\\DELL\\Desktop\\www-access.log-20190331")

    val count: Long = data.count()

    println(count)

    val prefix:DataSet[Tuple2[String,Int]]=data.map(x=>{

      val index: Int = x.indexOf("-")

      (x.substring(0,index),1)

    })

   val groupCount=prefix.groupBy(_._1).reduce((x,y)=>{

     val result=x._2+y._2

     (x._1,result)
   })



    val end: DataSet[Int] = groupCount.map(_._2).reduce(_+_)

    println(end.collect())











  }

}
