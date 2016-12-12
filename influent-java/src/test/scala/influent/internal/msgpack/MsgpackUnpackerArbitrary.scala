package influent.internal.msgpack

import org.msgpack.value._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import scala.collection.JavaConverters._

object MsgpackUnpackerArbitrary {
  implicit lazy val arbValue: Arbitrary[ImmutableValue] = Arbitrary(genValue(0))

  private[this] def genValue(level: Int): Gen[ImmutableValue] = {
    def genScalar: Gen[ImmutableValue] = Gen.oneOf(
      arbitrary[ImmutableBinaryValue],
      arbitrary[ImmutableBooleanValue],
      arbitrary[ImmutableIntegerValue],
      arbitrary[ImmutableFloatValue],
      arbitrary[ImmutableNilValue],
      arbitrary[ImmutableStringValue]
    )
    def genCollection(level: Int): Gen[ImmutableValue] = Gen.oneOf(
      genArray(level),
      genMap(level)
    )
    level match {
      case 2 => genScalar
      case x => Gen.frequency(50 -> genScalar, 1 -> genCollection(x + 1))
    }
  }

  implicit lazy val arbBinary: Arbitrary[ImmutableBinaryValue] = Arbitrary {
    Gen.listOf(Arbitrary.arbByte.arbitrary).map(_.toArray).map(ValueFactory.newBinary)
  }

  implicit lazy val arbBoolean: Arbitrary[ImmutableBooleanValue] = Arbitrary {
    arbitrary[Boolean].map(ValueFactory.newBoolean)
  }

  implicit lazy val arbInteger: Arbitrary[ImmutableIntegerValue] = Arbitrary(Gen.oneOf(
    arbitrary[Long].map(ValueFactory.newInteger),
    arbitrary[BigInt].filter { value =>
      value.bitLength <= 63 || value.bitLength == 64 && value.signum == 1
    }.map(_.bigInteger).map(ValueFactory.newInteger)
  ))

  implicit lazy val arbFloat: Arbitrary[ImmutableFloatValue] = Arbitrary(Gen.oneOf(
    arbitrary[Float].map(ValueFactory.newFloat),
    arbitrary[Double].map(ValueFactory.newFloat)
  ))

  implicit lazy val arbNil: Arbitrary[ImmutableNilValue] = Arbitrary(Gen.const(ValueFactory.newNil()))

  implicit lazy val arbString: Arbitrary[ImmutableStringValue] = Arbitrary {
    Gen.alphaStr.map(ValueFactory.newString)
  }

  private[this] def genArray(level: Int): Gen[ImmutableArrayValue] = {
    Gen.listOf(genValue(level)).map(_.asJava).map(ValueFactory.newArray)
  }

  implicit lazy val arbArray: Arbitrary[ImmutableArrayValue] = Arbitrary(genArray(0))

  private[this] def genMap(level: Int): Gen[ImmutableMapValue] = {
    val genKV = for {
      k <- genValue(level)
      v <- genValue(level)
    } yield (k, v)
    Gen.mapOf(genKV).flatMap { kvs =>
      kvs.map { case (k, v) => (k: Value, v: Value) }
    }.map(_.asJava).map { x => ValueFactory.newMap(x) }
  }

  implicit lazy val arbMap: Arbitrary[ImmutableMapValue] = Arbitrary(genMap(0))
}
