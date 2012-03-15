object DebugPrinter {
  val isDebug = true
  def print(x: Any) {
    if (isDebug) Predef.print(x)
  }

  def println(x: Any) {
    if (isDebug) Predef.println(x)
  }

  def println() {
    if (isDebug) Predef.println()
  }
}
