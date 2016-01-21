class ClassOne[T](val input: T) {
  def display() {
    println("ClassOne: " + input)
  }
}

val aClassOne = new ClassOne("first")
aClassOne.display // -> ClassOne: first
aClassOne.input // -> first

class ClassOneStr(val aClassOneObjStr: ClassOne[String]) {
  def duplicateString() = aClassOneObjStr.input + aClassOneObjStr.input
}

val aClassOneStr = new ClassOneStr(aClassOne)
aClassOneStr.duplicateString  // -> firstfirst

val anotherClassOne = new ClassOne(123)
anotherClassOne.display // -> ClassOne: 123
anotherClassOne.input // -> 123

class ClassOneInt(val aClassOneObjInt: ClassOne[Int]) {
  def duplicateInt() = aClassOneObjInt.input.toString + aClassOneObjInt.input.toString
}

val aClassOneInt = new ClassOneInt(anotherClassOne)
aClassOneInt.duplicateString  // -> 123123

implicit def toStrMethods(aClassOneStrObj: ClassOne[String]) = new ClassOneStr(aClassOneStrObj)
implicit def toIntMethods(aClassOneIntObj: ClassOne[Int]) = new ClassOneInt(aClassOneIntObj)

val oneStrTest = new ClassOne("test")
oneStrTest.duplicateString // -> testtest
oneStrTest.duplicateInt // -> ERROR: duplicateInt is not a memer of ClassOne

val oneIntTest = new ClassOne(321)
oneIntTest.duplicateInt
oneIntTest.duplicateString
