package influent.forward

import influent._
import influent.exception.InfluentIOException
import influent.internal.msgpack.MsgpackStreamUnpacker
import influent.internal.nio.{NioEventLoop, NioTcpChannel}
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import org.mockito.AdditionalAnswers
import org.mockito.Mockito._
import org.mockito.stubbing.Answer1
import org.msgpack.core.MessagePack
import org.msgpack.value.impl.ImmutableStringValueImpl
import org.scalatest.WordSpec
import org.scalatest.mockito.MockitoSugar

class NioForwardConnectionSpec extends WordSpec with MockitoSugar {
  private[this] def success: CompletableFuture[Void] = {
    val future = new CompletableFuture[Void]()
    future.complete(null)
    future
  }
  private[this] def failure: CompletableFuture[Void] = {
    val future = new CompletableFuture[Void]()
    future.completeExceptionally(new RuntimeException)
    future
  }
  private[this] def response(chunk: String): ByteBuffer = {
    val stream = new ByteArrayOutputStream()
    val packer = MessagePack.newDefaultPacker(stream)
    packer.packMapHeader(1)
    packer.packString("ack")
    packer.packString(chunk)
    packer.flush()
    ByteBuffer.wrap(stream.toByteArray)
  }

  "onWritable" should {
    def createConnection(channel: NioTcpChannel, eventLoop: NioEventLoop): NioForwardConnection = {
      new NioForwardConnection(channel, eventLoop, mock[ForwardCallback], Int.MaxValue)
    }

    "send responses" in {
      val buffers = Seq(response("mofu1"), response("mofu2"), response("mofu3"))
      val channel = mock[NioTcpChannel]
      buffers.foreach { buffer =>
        when(channel.write(buffer))
          .thenAnswer(AdditionalAnswers.answer(new Answer1[Int, ByteBuffer] {
            override def answer(a: ByteBuffer): Int = {
              val size = a.remaining()
              a.position(a.limit())
              size
            }
          }))
      }

      val eventLoop = mock[NioEventLoop]

      val connection = createConnection(channel, eventLoop)
      buffers.foreach { buffer => connection.responses.enqueue(buffer) }
      val key = mock[SelectionKey]
      assert(connection.onWritable(key) === ())

      buffers.foreach { buffer =>
        buffer.rewind()
        verify(channel).write(buffer)
      }
      assert(!connection.responses.nonEmpty())
      verify(eventLoop).disableInterestSet(key, SelectionKey.OP_WRITE)
    }

    "not disable OP_WRITE" when {
      "all buffered responses are not sent" in {
        val buffers = Seq(response("mofu1"), response("mofu2"), response("mofu3"))
        val channel = mock[NioTcpChannel]
        when(channel.write(buffers(0)))
          .thenAnswer(AdditionalAnswers.answer(new Answer1[Int, ByteBuffer] {
            override def answer(a: ByteBuffer): Int = {
              val size = a.remaining()
              a.position(a.limit())
              size
            }
          }))
        when(channel.write(buffers(1)))
          .thenAnswer(AdditionalAnswers.answer(new Answer1[Int, ByteBuffer] {
            override def answer(a: ByteBuffer): Int = {
              val size = a.remaining() - 1
              a.position(a.limit() - 1)
              size
            }
          }))

        val eventLoop = mock[NioEventLoop]

        val connection = createConnection(channel, eventLoop)

        buffers.foreach { buffer => connection.responses.enqueue(buffer) }
        val key = mock[SelectionKey]
        assert(connection.onWritable(key) === ())

        assert(buffers(0).remaining() === 0)
        assert(buffers(1).remaining() === 1)
        assert(buffers(2).remaining() === buffers(2).limit())

        assert(connection.responses.dequeue() eq buffers(1))
        assert(connection.responses.dequeue() eq buffers(2))
        assert(!connection.responses.nonEmpty())

        buffers.foreach(_.rewind())
        verify(channel).write(buffers(0))
        verify(channel).write(buffers(1))
        verifyNoMoreInteractions(channel)
        verifyZeroInteractions(eventLoop)
      }
    }

    "fail with InfluentIOException" when {
      "it fails writing" in {
        val channel = mock[NioTcpChannel]
        when(channel.write(response("mofu"))).thenThrow(new InfluentIOException())

        val eventLoop = mock[NioEventLoop]
        val connection = createConnection(channel, eventLoop)

        connection.responses.enqueue(response("mofu"))
        assertThrows[InfluentIOException](connection.onWritable(mock[SelectionKey]))

        verify(channel).write(response("mofu"))
        verifyZeroInteractions(eventLoop)
      }
    }
  }

  "onReadable" should {
    "receive requests" in {
      val channel = mock[NioTcpChannel]

      val unpacker = mock[MsgpackStreamUnpacker]
      when(unpacker.hasNext).thenReturn(true, true, true, true, false)
      val dummyValue = new ImmutableStringValueImpl("mofu")
      when(unpacker.next()).thenReturn(dummyValue)
      when(channel.isOpen).thenReturn(true)

      val decoder = mock[MsgpackForwardRequestDecoder]
      val requests = Seq(
        Optional.of(ForwardRequest.of(
          EventStream.of(Tag.of("mofu1"), new util.LinkedList[EventEntry]()),
          ForwardOption.of("chunk1", null)
        )),
        Optional.of(ForwardRequest.of(
          EventStream.of(Tag.of("mofu2"), new util.LinkedList[EventEntry]()),
          ForwardOption.of(null, null)
        )),
        Optional.empty[ForwardRequest](),
        Optional.of(ForwardRequest.of(
          EventStream.of(Tag.of("mofu3"), new util.LinkedList[EventEntry]()),
          ForwardOption.of("chunk3", null)
        ))
      )
      requests.foldLeft(when(decoder.decode(dummyValue))) { (stub, request) =>
        stub.thenReturn(request)
      }

      val callback = mock[ForwardCallback]
      requests.filter(_.isPresent).foreach { request =>
        when(callback.consume(request.get().getStream)).thenReturn(success)
      }

      val eventLoop = mock[NioEventLoop]
      val connection = new NioForwardConnection(channel, eventLoop, callback, unpacker, decoder)
      val key = mock[SelectionKey]
      assert(connection.onReadable(key) === ())

      requests.filter(_.isPresent).foreach { request =>
        verify(callback).consume(request.get().getStream)
      }
      assert(connection.responses.dequeue() === response("chunk1"))
      assert(connection.responses.dequeue() === response("chunk3"))
      assert(!connection.responses.nonEmpty())
      verify(eventLoop, times(2)).enableInterestSet(key, SelectionKey.OP_WRITE)
      verify(channel, never()).close()
    }

    "send responses asynchronously" in {
      val channel = mock[NioTcpChannel]

      val unpacker = mock[MsgpackStreamUnpacker]
      when(unpacker.hasNext).thenReturn(true, false)
      val dummyValue = new ImmutableStringValueImpl("mofu")
      when(unpacker.next()).thenReturn(dummyValue)

      val decoder = mock[MsgpackForwardRequestDecoder]
      val request = ForwardRequest.of(
        EventStream.of(Tag.of("mofu1"), new util.LinkedList[EventEntry]()),
        ForwardOption.of("chunk1", null)
      )
      when(decoder.decode(dummyValue)).thenReturn(Optional.of(request))

      val callback = mock[ForwardCallback]
      when(callback.consume(request.getStream))
        .thenAnswer(AdditionalAnswers.answer(new Answer1[CompletableFuture[Void], EventStream] {
          override def answer(a: EventStream): CompletableFuture[Void] = {
            CompletableFuture.runAsync(new Runnable {
              override def run(): Unit = {
                Thread.sleep(1000)
              }
            })
          }
        }))

      val eventLoop = mock[NioEventLoop]
      val connection = new NioForwardConnection(channel, eventLoop, callback, unpacker, decoder)
      val key = mock[SelectionKey]
      assert(connection.onReadable(key) === ())
      verify(callback).consume(request.getStream)
      assert(!connection.responses.nonEmpty())

      Thread.sleep(1500)
      assert(connection.responses.nonEmpty())
      assert(connection.responses.dequeue() === response("chunk1"))
    }

    "not enable OP_WRITE" when {
      "all the request has no option" in {
        val channel = mock[NioTcpChannel]

        val unpacker = mock[MsgpackStreamUnpacker]
        when(unpacker.hasNext).thenReturn(true, false)
        val dummyValue = new ImmutableStringValueImpl("mofu")
        when(unpacker.next()).thenReturn(dummyValue)

        val decoder = mock[MsgpackForwardRequestDecoder]
        val request = ForwardRequest.of(
          EventStream.of(Tag.of("mofu1"), new util.LinkedList[EventEntry]()),
          ForwardOption.of(null, null)
        )
        when(decoder.decode(dummyValue)).thenReturn(Optional.of(request))

        val callback = mock[ForwardCallback]
        when(callback.consume(request.getStream)).thenReturn(success)

        val eventLoop = mock[NioEventLoop]
        val connection = new NioForwardConnection(channel, eventLoop, callback, unpacker, decoder)
        val key = mock[SelectionKey]
        assert(connection.onReadable(key) === ())
        verify(callback).consume(request.getStream)
        verifyZeroInteractions(eventLoop)
      }
    }

    "close" when {
      "the stream terminates" in {
        val channel = mock[NioTcpChannel]
        val unpacker = mock[MsgpackStreamUnpacker]
        when(unpacker.hasNext).thenReturn(false)
        when(channel.isOpen).thenReturn(false)

        val eventLoop = mock[NioEventLoop]
        val callback = mock[ForwardCallback]
        val decoder = mock[MsgpackForwardRequestDecoder]
        val connection = new NioForwardConnection(channel, eventLoop, callback, unpacker, decoder)

        assert(connection.onReadable(mock[SelectionKey]) === ())
        verify(channel).close()
      }
    }

    "don't response" when {
      "the future fails" in {
        val channel = mock[NioTcpChannel]

        val unpacker = mock[MsgpackStreamUnpacker]
        when(unpacker.hasNext).thenReturn(true, false)
        val dummyValue = new ImmutableStringValueImpl("mofu")
        when(unpacker.next()).thenReturn(dummyValue)

        val decoder = mock[MsgpackForwardRequestDecoder]
        val request = ForwardRequest.of(
          EventStream.of(Tag.of("mofu1"), new util.LinkedList[EventEntry]()),
          ForwardOption.of("chunk1", null)
        )
        when(decoder.decode(dummyValue)).thenReturn(Optional.of(request))

        val callback = mock[ForwardCallback]
        when(callback.consume(request.getStream)).thenReturn(failure)

        val eventLoop = mock[NioEventLoop]
        val connection = new NioForwardConnection(channel, eventLoop, callback, unpacker, decoder)
        val key = mock[SelectionKey]
        assert(connection.onReadable(key) === ())
        verify(callback).consume(request.getStream)

        assert(!connection.responses.nonEmpty())
        verifyZeroInteractions(eventLoop)
      }
    }

    "ignore invalid requests" in {
      val channel = mock[NioTcpChannel]

      val unpacker = mock[MsgpackStreamUnpacker]
      when(unpacker.hasNext).thenReturn(true, true, false)
      val dummyValue = new ImmutableStringValueImpl("mofu")
      when(unpacker.next()).thenReturn(dummyValue)

      val decoder = mock[MsgpackForwardRequestDecoder]
      val request = ForwardRequest.of(
        EventStream.of(Tag.of("mofu1"), new util.LinkedList[EventEntry]()),
        ForwardOption.of("chunk1", null)
      )
      when(decoder.decode(dummyValue))
        .thenThrow(new IllegalArgumentException)
        .thenReturn(Optional.of(request))

      val callback = mock[ForwardCallback]
      when(callback.consume(request.getStream)).thenReturn(success)

      val eventLoop = mock[NioEventLoop]
      val connection = new NioForwardConnection(channel, eventLoop, callback, unpacker, decoder)
      val key = mock[SelectionKey]
      assert(connection.onReadable(key) === ())

      verify(callback).consume(request.getStream)
      assert(connection.responses.dequeue() === response("chunk1"))
      assert(!connection.responses.nonEmpty())
      verify(eventLoop, times(1)).enableInterestSet(key, SelectionKey.OP_WRITE)
    }

    "fail with InfluentIOException" when {
      "it fails reading" in {
        val callback = mock[ForwardCallback]
        val channel = mock[NioTcpChannel]
        val unpacker = mock[MsgpackStreamUnpacker]
        when(unpacker.read(channel)).thenThrow(new InfluentIOException())
        val connection = new NioForwardConnection(
          channel, mock[NioEventLoop], callback, unpacker, mock[MsgpackForwardRequestDecoder]
        )

        assertThrows[InfluentIOException](connection.onReadable(mock[SelectionKey]))
        verifyZeroInteractions(callback)
      }
    }
  }

  "close" should {
    "closes the channel" in {
      val channel = mock[NioTcpChannel]
      val connection = new NioForwardConnection(
        channel, mock[NioEventLoop], mock[ForwardCallback], Int.MaxValue
      )
      assert(connection.close() === ())
      verify(channel).close()
    }
  }
}
