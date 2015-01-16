name := "cg"

version := "1.0"

scalaVersion := "2.10.4"

organization := "himrod"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.0" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := "cg-assembly.jar"

assemblyMergeStrategy in assembly := { 
	case x if Assembly.isConfigFile(x) =>
		MergeStrategy.concat
	case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
		MergeStrategy.rename
	case PathList("META-INF", xs @ _*) =>
		(xs map {_.toLowerCase}) match {
			case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
				MergeStrategy.discard
			case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
				MergeStrategy.discard
			case "plexus" :: xs =>
				MergeStrategy.discard
			case "services" :: xs =>
				MergeStrategy.filterDistinctLines
			case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
				MergeStrategy.filterDistinctLines
			case _ => MergeStrategy.deduplicate
		}
	case _ => MergeStrategy.deduplicate
}
