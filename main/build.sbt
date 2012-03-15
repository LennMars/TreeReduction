name := "TreeReduction"

version := "0.1"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked"
)

scalaSource in Compile <<= baseDirectory(_ / "src")

mainClass in (Compile, run) := Some("TreeReduction")