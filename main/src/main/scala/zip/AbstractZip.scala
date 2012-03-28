trait AbstractZip {
  type t1
  type t2
  type t3
  def extract1(s: String): List[Option[t1]]
  def extract2(s: String): List[Option[t2]]
  val zipper: (t1, t2) => t3
  val t1OfString: String => t1
  val t2OfString: String => t2
  val t3OfString: String => t3
  def t1ToString(x: t1): String
  def t2ToString(x: t2): String
  def t3ToString(x: t3): String
}
