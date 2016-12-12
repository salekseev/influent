package influent.internal.msgpack;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import org.msgpack.core.MessagePack;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.ValueFactory;

abstract class MsgpackIncrementalUnpacker {
  abstract DecodeResult unpack(final InfluentByteBuffer buffer);
}

final class FormatUnpacker extends MsgpackIncrementalUnpacker {
  private static final FormatUnpacker INSTANCE = new FormatUnpacker();

  private FormatUnpacker() {
  }

  static FormatUnpacker getInstance() {
    return INSTANCE;
  }

  @Override
  DecodeResult unpack(final InfluentByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return DecodeResult.next(this);
    }

    final byte head = buffer.getByte();
    if (MessagePack.Code.isFixInt(head)) {
      // positive fixint or negative fixint
      return DecodeResult.complete(ValueFactory.newInteger(head));
    }
    if (MessagePack.Code.isFixedMap(head)) {
      return MultipleUnpacker.map(head & 0x0f).unpack(buffer);
    }
    if (MessagePack.Code.isFixedArray(head)) {
      return MultipleUnpacker.array(head & 0x0f).unpack(buffer);
    }
    if (MessagePack.Code.isFixedRaw(head)) {
      return ConstantUnpacker.string(head & 0x1f).unpack(buffer);
    }

    switch (head) {
      case MessagePack.Code.NIL:
        return DecodeResult.complete(ValueFactory.newNil());
      case MessagePack.Code.FALSE:
        return DecodeResult.complete(ValueFactory.newBoolean(false));
      case MessagePack.Code.TRUE:
        return DecodeResult.complete(ValueFactory.newBoolean(true));
      case MessagePack.Code.BIN8:
        return SizeUnpacker.int8(ConstantUnpacker::binary).unpack(buffer);
      case MessagePack.Code.BIN16:
        return SizeUnpacker.int16(ConstantUnpacker::binary).unpack(buffer);
      case MessagePack.Code.BIN32:
        return SizeUnpacker.int32(ConstantUnpacker::binary).unpack(buffer);
      case MessagePack.Code.FLOAT32:
        return new ConstantUnpacker(Float.BYTES, bytes -> ValueFactory.newFloat(bytes.getFloat()))
            .unpack(buffer);
      case MessagePack.Code.FLOAT64:
        return new ConstantUnpacker(Double.BYTES, bytes -> ValueFactory.newFloat(bytes.getDouble()))
            .unpack(buffer);
      case MessagePack.Code.UINT8:
        return new ConstantUnpacker(1, bytes -> ValueFactory.newInteger(bytes.get() & 0xff))
            .unpack(buffer);
      case MessagePack.Code.UINT16:
        return new ConstantUnpacker(2, bytes -> ValueFactory.newInteger(bytes.getShort() & 0xffff))
            .unpack(buffer);
      case MessagePack.Code.UINT32:
        return new ConstantUnpacker(4, bytes -> {
          final int intValue = bytes.getInt();
          final long value = intValue < 0 ? (intValue & 0x7fffffff) + 0x80000000L : intValue;
          return ValueFactory.newInteger(value);
        }).unpack(buffer);
      case MessagePack.Code.UINT64:
        return new ConstantUnpacker(8, bytes -> {
          final long longValue = bytes.getLong();
          if (longValue < 0L) {
            final BigInteger value = BigInteger.valueOf(longValue + Long.MAX_VALUE + 1L).setBit(63);
            return ValueFactory.newInteger(value);
          } else {
            return ValueFactory.newInteger(longValue);
          }
        }).unpack(buffer);
      case MessagePack.Code.INT8:
        return new ConstantUnpacker(1, bytes -> ValueFactory.newInteger(bytes.get()))
            .unpack(buffer);
      case MessagePack.Code.INT16:
        return new ConstantUnpacker(2, bytes -> ValueFactory.newInteger(bytes.getShort()))
            .unpack(buffer);
      case MessagePack.Code.INT32:
        return new ConstantUnpacker(4, bytes -> ValueFactory.newInteger(bytes.getInt()))
            .unpack(buffer);
      case MessagePack.Code.INT64:
        return new ConstantUnpacker(8, bytes -> ValueFactory.newInteger(bytes.getLong()))
            .unpack(buffer);
      case MessagePack.Code.STR8:
        return SizeUnpacker.int8(ConstantUnpacker::string).unpack(buffer);
      case MessagePack.Code.STR16:
        return SizeUnpacker.int16(ConstantUnpacker::string).unpack(buffer);
      case MessagePack.Code.STR32:
        return SizeUnpacker.int32(ConstantUnpacker::string).unpack(buffer);
      case MessagePack.Code.ARRAY16:
        return SizeUnpacker.int16(MultipleUnpacker::array).unpack(buffer);
      case MessagePack.Code.ARRAY32:
        return SizeUnpacker.int32(MultipleUnpacker::array).unpack(buffer);
      case MessagePack.Code.MAP16:
        return SizeUnpacker.int16(MultipleUnpacker::map).unpack(buffer);
      case MessagePack.Code.MAP32:
        return SizeUnpacker.int32(MultipleUnpacker::map).unpack(buffer);
      case MessagePack.Code.EXT8:
      case MessagePack.Code.EXT16:
      case MessagePack.Code.EXT32:
      case MessagePack.Code.FIXEXT1:
      case MessagePack.Code.FIXEXT2:
      case MessagePack.Code.FIXEXT4:
      case MessagePack.Code.FIXEXT8:
        throw new RuntimeException("Influent does not support ext format.");
      default:
        throw new RuntimeException("Unknown format is given.");
    }
  }
}

final class ConstantUnpacker extends MsgpackIncrementalUnpacker {
  private final Function<ByteBuffer, ImmutableValue> factory;
  private final ByteBuffer builder;

  ConstantUnpacker(final int size, final Function<ByteBuffer, ImmutableValue> factory) {
    this.builder = ByteBuffer.allocate(size);
    this.factory = factory;
  }

  static ConstantUnpacker binary(final int size) {
    return new ConstantUnpacker(size, bytes -> ValueFactory.newBinary(bytes.array(), true));
  }

  static ConstantUnpacker string(final int size) {
    return new ConstantUnpacker(size, bytes -> ValueFactory.newString(bytes.array(), true));
  }

  @Override
  DecodeResult unpack(final InfluentByteBuffer buffer) {
    buffer.get(builder);
    if (builder.hasRemaining()) {
      return DecodeResult.next(this);
    } else {
      builder.flip();
      return DecodeResult.complete(factory.apply(builder));
    }
  }
}

final class MultipleUnpacker extends MsgpackIncrementalUnpacker {
  private final ImmutableValue[] builder;
  private final Function<ImmutableValue[], ImmutableValue> factory;

  private int position = 0;
  private MsgpackIncrementalUnpacker current = FormatUnpacker.getInstance();

  private MultipleUnpacker(final int size, final Function<ImmutableValue[], ImmutableValue> factory) {
    this.builder = new ImmutableValue[size];
    this.factory = factory;
  }

  static MultipleUnpacker map(final int size) {
    return new MultipleUnpacker(size * 2, values -> ValueFactory.newMap(values, true));
  }

  static MultipleUnpacker array(final int size) {
    return new MultipleUnpacker(size, values -> ValueFactory.newArray(values, true));
  }

  @Override
  final DecodeResult unpack(final InfluentByteBuffer buffer) {
    while (position < builder.length) {
      final DecodeResult result = current.unpack(buffer);
      if (!result.isCompleted()) {
        current = result.next();
        return DecodeResult.next(this);
      }

      builder[position++] = result.value();
      current = FormatUnpacker.getInstance();
    }

    return DecodeResult.complete(factory.apply(builder));
  }
}

final class SizeUnpacker extends MsgpackIncrementalUnpacker {
  private final ByteBuffer dst;
  private final ToIntFunction<ByteBuffer> sizeConverter;
  private final IntFunction<MsgpackIncrementalUnpacker> factory;

  private static final ToIntFunction<ByteBuffer> INT8_CONVERTER = buffer -> buffer.get() & 0xff;
  private static final ToIntFunction<ByteBuffer> INT16_CONVERTER =
      buffer -> buffer.getShort() & 0xffff;
  private static final ToIntFunction<ByteBuffer> INT32_CONVERTER = buffer -> {
    final int size = buffer.getInt();
    if (size < 0) {
      throw new RuntimeException("The length exceeds Integer.MAX_VALUE.");
    }
    return size;
  };

  static SizeUnpacker int8(final IntFunction<MsgpackIncrementalUnpacker> factory) {
    return new SizeUnpacker(1, INT8_CONVERTER, factory);
  }
  static SizeUnpacker int16(final IntFunction<MsgpackIncrementalUnpacker> factory) {
    return new SizeUnpacker(2, INT16_CONVERTER, factory);
  }
  static SizeUnpacker int32(final IntFunction<MsgpackIncrementalUnpacker> factory) {
    return new SizeUnpacker(4, INT32_CONVERTER, factory);
  }

  private SizeUnpacker(final int bytes,
                       final ToIntFunction<ByteBuffer> converter,
                       final IntFunction<MsgpackIncrementalUnpacker> factory) {
    this.dst = ByteBuffer.allocate(bytes);
    this.sizeConverter = converter;
    this.factory = factory;
  }

  @Override
  DecodeResult unpack(final InfluentByteBuffer buffer) {
    buffer.get(dst);
    if (dst.hasRemaining()) {
      return DecodeResult.next(this);
    } else {
      dst.flip();
      final int size = sizeConverter.applyAsInt(dst);
      return factory.apply(size).unpack(buffer);
    }
  }
}
