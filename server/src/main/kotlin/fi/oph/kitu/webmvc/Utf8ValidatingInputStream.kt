package fi.oph.kitu.webmvc

import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

class InvalidUtf8Exception(
    val offset: Long,
) : CharacterCodingException() {
    override val message: String
        get() = "Pyynnön runko ei ole kelvollista UTF-8:aa (tavu $offset)"
}

class Utf8ValidatingInputStream(
    input: InputStream,
) : FilterInputStream(input) {
    private val decoder =
        Charsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
    private val pending = ByteBuffer.allocate(BUFFER_SIZE)
    private val chars = CharBuffer.allocate(BUFFER_SIZE)
    private var validatedBytes = 0L
    private var finished = false

    override fun read(): Int {
        val byte = ByteArray(1)
        return if (read(byte, 0, 1) == -1) -1 else byte[0].toInt() and 0xFF
    }

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int {
        val count = `in`.read(b, off, len)
        if (count == -1) {
            finish()
        } else {
            validate(b, off, count)
        }
        return count
    }

    override fun skip(n: Long): Long {
        val buffer = ByteArray(minOf(n, BUFFER_SIZE.toLong()).toInt().coerceAtLeast(1))
        var skipped = 0L
        while (skipped < n) {
            val count = read(buffer, 0, minOf(n - skipped, buffer.size.toLong()).toInt())
            if (count == -1) break
            skipped += count
        }
        return skipped
    }

    override fun markSupported(): Boolean = false

    override fun mark(readlimit: Int) {}

    override fun reset(): Unit = throw IOException("mark/reset not supported")

    private fun validate(
        b: ByteArray,
        off: Int,
        count: Int,
    ) {
        var position = off
        val end = off + count
        while (position < end) {
            val chunk = minOf(end - position, pending.remaining())
            pending.put(b, position, chunk)
            position += chunk
            decode(endOfInput = false)
        }
    }

    private fun finish() {
        if (finished) return
        finished = true
        decode(endOfInput = true)
        if (decoder.flush(chars).isError) throw InvalidUtf8Exception(validatedBytes)
    }

    private fun decode(endOfInput: Boolean) {
        pending.flip()
        while (true) {
            val start = pending.position()
            val result = decoder.decode(pending, chars, endOfInput)
            validatedBytes += pending.position() - start
            chars.clear()
            if (result.isError) throw InvalidUtf8Exception(validatedBytes)
            if (result.isUnderflow) break
        }
        pending.compact()
    }

    private companion object {
        const val BUFFER_SIZE = 8192
    }
}
