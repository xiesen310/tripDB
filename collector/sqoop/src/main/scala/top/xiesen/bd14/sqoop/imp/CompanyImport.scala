package top.xiesen.bd14.sqoop.imp

import org.apache.sqoop.client.SqoopClient
import org.apache.sqoop.model.{MFromConfig, MInputType, MToConfig}

object CompanyImport {

  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)

  // 注意sqoop的lib下目录把postgresql的jar copy

  // 创建job job中把Company的数据导入到hdfs上
  // 启动job

  def createJob() = {
    val job = client.createJob("postgresql-link","hdfs-link")
    job.setName("btrip_company")
    val sql =
      """
        |select company_id
        |   ,company_address
        |   ,company_attr
        |   ,company_boss
        |   ,company_name
        |   ,company_phone
        |from wsc.tb_company
        |${CONDITIONS}
      """.stripMargin
    val fromConfig = job.getFromJobConfig()
    val toConfig = job.getToJobConfig
    showFromJobConfig(fromConfig)
    showToJobConfig(toConfig)

//    fromConfig.getStringInput("fromJobConfig.schemaName").setValue("wsc")
//    fromConfig.getStringInput("fromJobConfig.tableName").setValue("tb_company")
    fromConfig.getStringInput("fromJobConfig.sql").setValue(sql)
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue("company_id")

    toConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE")
    toConfig.getEnumInput("toJobConfig.compression").setValue("NONE")
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue("/sqoop/btrip-pg")
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true)

    val status = client.saveJob(job)
    if(status.canProceed){
      println("创建company job成功")
    }else{
      println("创建company job失败")
    }
  }

  // 展示from数据配置项
  def showFromJobConfig(configs:MFromConfig) ={
    val configList = configs.getConfigs
    for(i <- 0 until configList.size()){
      val config = configList.get(i)
      val inputs = config.getInputs
      for(j <- 0 until inputs.size()){
        val input = inputs.get(j)
        println(input)
      }
    }
  }

  // 展示to数据配置项
  def showToJobConfig(configs:MToConfig)={
    val configList = configs.getConfigs
    for(i <- 0 until configList.size()){
      val config = configList.get(i)
      val inputs = config.getInputs
      for(j <- 0 until inputs.size()){
        val input = inputs.get(j)
        println(input)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    createJob()
  }
}
