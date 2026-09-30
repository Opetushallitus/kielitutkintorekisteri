package fi.oph.kitu.oid

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import fi.oph.kitu.util.result.getOrThrow
import io.swagger.v3.oas.annotations.media.Schema
import org.ietf.jgss.GSSException
import tools.jackson.databind.annotation.JsonSerialize
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.sql.ResultSet

@ConsistentCopyVisibility
@JsonSerialize(using = tools.jackson.databind.ser.std.ToStringSerializer::class)
@Schema(type = "string")
data class Oid private constructor(
    private var value: org.ietf.jgss.Oid,
) : Serializable {
    override fun toString(): String = value.toString()

    constructor(valueString: String) : this(org.ietf.jgss.Oid(valueString))

    @Throws(IOException::class)
    private fun writeObject(out: ObjectOutputStream) {
        out.writeUTF(value.toString())
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    private fun readObject(inp: ObjectInputStream) {
        val str = inp.readUTF()
        value = org.ietf.jgss.Oid(str)
    }

    companion object {
        fun String.isOidOfClass(oidClass: OidClass): Boolean = startsWith("${oidClass.node}.") && parse(this).isRight()

        fun parse(source: String?): Either<MalformedOidError, Oid> =
            try {
                Oid(org.ietf.jgss.Oid(source)).right()
            } catch (_: GSSException) {
                MalformedOidError(source).left()
            }
    }
}

data class MalformedOidError(
    val source: String?,
) : Exception("Malformed Oid \"$source\"")

fun ResultSet.getOid(columnLabel: String): Oid = Oid.parse(getString(columnLabel)).getOrThrow()

fun ResultSet.getOidOrNull(columnLabel: String): Oid? = getString(columnLabel)?.let { Oid.parse(it).getOrThrow() }
