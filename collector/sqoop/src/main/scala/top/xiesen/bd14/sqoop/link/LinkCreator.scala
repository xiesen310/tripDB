package top.xiesen.bd14.sqoop.link

import org.apache.sqoop.client.SqoopClient
import org.apache.sqoop.model.{MConfig, MInput}

/**
  * 创建link
  */
object LinkCreator {
  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)

  // 创建hdfsClient
  def createHdfsLink() = {
    val hdfsLink = client.createLink("hdfs-connector")
    hdfsLink.setName("hdfs-link")
    val linkConfig = hdfsLink.getConnectorLinkConfig()
    val configs: java.util.List[MConfig] = linkConfig.getConfigs
    printLinkConfiguration(configs)

    linkConfig.getStringInput("linkConfig.uri").setValue("hdfs://master:9000")
    // hadoop配置文件路径
    linkConfig.getStringInput("linkConfig.confDir").setValue("/opt/software/hadoop/hadoop-2.7.4/etc/hadoop")
    //    linkConfig.getMapInput("input-linkConfig.configOverrides").setValue()
    val status = client.saveLink(hdfsLink)
    if (status.canProceed) {
      println("hdfs-link创建成功")
    } else {
      println("hdfs-link创建失败")
    }
  }

  // 创建postgresql link ----> jdbc link
  def createPostgresqlLink() = {
    val pglink = client.createLink("generic-jdbc-connector")
    pglink.setName("postgresql-link")
    val linkConfig = pglink.getConnectorLinkConfig()
    printLinkConfiguration(linkConfig.getConfigs)

    linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("org.postgresql.Driver")
    linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:postgresql://192.168.159.1:5432/WscHMS")
    linkConfig.getStringInput("linkConfig.username").setValue("postgres")
    linkConfig.getStringInput("linkConfig.password").setValue("root")
    linkConfig.getStringInput("dialect.identifierEnclose").setValue(" ")

    val status = client.saveLink(pglink)
    if (status.canProceed) {
      println("postgresql-link创建成功")
    } else {
      println("postgresql-link创建失败")
    }
  }

  // 打印link的配置项参数名称和参数类型等信息，方便link配置
  def printLinkConfiguration(configs: java.util.List[MConfig]) = {
    for (i <- 0 until configs.size()) {
      val inputs: java.util.List[MInput[_]] = configs.get(i).getInputs
      for (j <- 0 until inputs.size()) {
        val input = inputs.get(j)
        println(input)
      }
    }
  }
  def main(args: Array[String]): Unit = {
//    createHdfsLink()
    createPostgresqlLink()
  }
}
