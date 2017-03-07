package biz.meetmatch.util

import java.io.File
import java.lang.reflect.{Member, Modifier}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import biz.meetmatch.modules.Module
import org.apache.commons.io.FileUtils
import org.reflections.Reflections
import org.reflections.scanners.{MemberUsageScanner, MethodParameterNamesScanner, MethodParameterScanner, SubTypesScanner}
import org.reflections.util.{ClasspathHelper, ConfigurationBuilder}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

object DataFlow {
  private val logger: Logger = {
    Utils.setBusLogFileNamePropertyIfEmpty()
    LoggerFactory.getLogger(this.getClass)
  }

  val modulesPkg = "biz.meetmatch.modules"
  val reflections = new Reflections(
    new ConfigurationBuilder()
      .setUrls(ClasspathHelper.forPackage(modulesPkg))
      .setScanners(
        new SubTypesScanner(false),
        new MethodParameterScanner(),
        new MethodParameterNamesScanner(),
        new MemberUsageScanner()
      )
  )

  def main(args: Array[String]): Unit = {
    val mode = args.headOption.getOrElse("json")
    if (mode == "neo4j")
      printModuleDependenciesForNeo4j()
    else
      exportModuleDependenciesToJson()
  }

  def isAbstractClass(clas: Class[_]): Boolean = Modifier.isAbstract(clas.getModifiers)

  def getModules: mutable.Set[Class[_ <: Module]] = {
    reflections
      .getSubTypesOf(classOf[Module])
      .asScala
      .filter(_.getPackage.getName == modulesPkg)
  }

  def getAllConcreteSubClasses(clas: Class[_]): Array[Class[_]] = {
    reflections.getSubTypesOf(clas)
      .asScala
      .toArray
      .flatMap { subClass =>
        (if (isAbstractClass(subClass)) Array() else Array(subClass)) ++ getAllConcreteSubClasses(subClass)
      }
  }

  def getAllModuleDependencies: Array[(String, String)] = {
    getModules
      .toArray
      .filterNot(isAbstractClass)
      .flatMap(getModuleDependencies)
      .distinct
  }

  def getModuleDependencies(module: Class[_ <: Module]): Seq[(String, String)] = {
    println(module.getName + ":")
    val dependencyMethodNames = Array("loadResultsFromParquet", "loadResultsFromParquetAsDF", "getResultsFileLocation")
    val interestingMethods = module.getMethods
      .filter(method => dependencyMethodNames.contains(method.getName))

    val dependentModules = interestingMethods.flatMap { method =>
      println("\t" + method.getName + ":")
      val directlyDependentModules = reflections
        .getMethodUsage(method).asScala.toSet[Member] // this also includes all subclasses, superclasses and subclasses of the superclasses that directly or indirectly implement that same method
        .filter(_.getName != method.getName) // remove all these subclasses, superclasses and subclasses of the superclasses
        .map(member => (member.getDeclaringClass, member.getName))
        .filter { case (dependentModule, memberName) => dependentModule != method.getDeclaringClass } // don't allow the module of the method itself as a dependent module (would generate unwanted dependencies between its subclasses)

      directlyDependentModules.foreach { case (dependentModule, memberName) =>
        println("\t\tdirectly depending module: " + dependentModule.getName + " " + memberName)
      }

      val allDependentModules = directlyDependentModules
        .flatMap { case (dependentModule, memberName) =>
          (if (isAbstractClass(dependentModule)) Array() else Array(dependentModule)) ++
            getAllConcreteSubClasses(dependentModule).filter(dependentSubModule => usesMethodOfSuperClass(memberName, dependentSubModule, dependentModule))
        }

      val allDependentModulesFiltered = allDependentModules
        .filterNot(dependentModule => dependentModule == module)
        .filter(_.getPackage.getName == modulesPkg)

      allDependentModulesFiltered.foreach { dependentModule =>
        println("\t\tindirectly depending module: " + dependentModule.getName)
      }

      allDependentModulesFiltered
    }

    dependentModules.map { dependentModule => (formatModuleName(module.getName), formatModuleName(dependentModule.getName)) }
  }

  def usesMethodOfSuperClass(method: String, clas: Class[_], superClas: Class[_]): Boolean = {
    clas.getMethods
      .find(_.getName == method)
      .forall { executeMethod =>
        executeMethod.getDeclaringClass == superClas || // either the class simply inherits the method from the superclass
          superClas.getMethods.find(_.getName == "execute").exists { method => reflections.getMethodUsage(method).asScala.toSet[Member].map(_.getDeclaringClass).contains(clas) } //or it overrides it and calls it explicitly
      }
  }

  def exportModuleDependenciesToJson(): Unit = {
    val nodes = getModules
      .filterNot(isAbstractClass)
      .map { module =>
        val moduleName = formatModuleName(module.getName)
        """{"data": {"id":"""" + moduleName.toLowerCase + """", "label": """" + moduleName.replaceAll("(.)([A-Z])", "$1 $2") + """"}}"""
      }
      .mkString(",\n")

    val edges = getAllModuleDependencies
      .map { case (module, dep) => """{"data": {"source":"""" + module.toLowerCase + """", "target": """" + dep.toLowerCase + """"}}""" }
      .mkString(",\n")

    val json = s"""{"nodes": [ $nodes ], "edges": [ $edges ] }"""

    val path = Utils.getTextFileRoot + "/data-flow.json"
    val file = new File(path)

    FileUtils.forceMkdir(file.getParentFile)

    if (file.exists)
      FileUtils.forceDelete(file)

    Files.write(Paths.get(path), json.getBytes(StandardCharsets.UTF_8))
    println("The data flow was exported to " + path)
  }

  def printModuleDependenciesForNeo4j(): Unit = {
    getModules
      .filterNot(isAbstractClass)
      .foreach { module =>
        val moduleName = formatModuleName(module.getName)
        println("MERGE (" + moduleName.toLowerCase + ":Module{name: '" + moduleName + "', label: '" + moduleName.replaceAll("(.)([A-Z])", "$1 $2") + "'})")
      }

    getAllModuleDependencies.foreach { case (module, dep) => println("MERGE (" + module.toLowerCase + ")-[:NEXT]->(" + dep.toLowerCase + ")") }

    println()
    println("run the following command separately to visualize the nodes and relations:")
    println("MATCH (n) RETURN n")
  }

  private def formatModuleName(moduleName: String): String = moduleName.replace(modulesPkg + ".", "").split("\\$\\$").head.replace("$", "")
}
