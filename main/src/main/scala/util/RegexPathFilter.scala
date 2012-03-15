import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter

class RegexPathFilter(val regex: String) extends PathFilter {
  override def accept(path: Path): Boolean = {
    path.toString.matches(regex)
  }
}
