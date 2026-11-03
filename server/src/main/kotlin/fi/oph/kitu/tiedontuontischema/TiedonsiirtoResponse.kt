package fi.oph.kitu.tiedontuontischema

import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ResponseEntity

interface TiedonsiirtoResponse {
    val result: TiedonsiirtoStatus

    fun toResponseEntity(): ResponseEntity<*>
}

enum class TiedonsiirtoStatus {
    OK,
    Failed,
}

class TiedonsiirtoSuccess : TiedonsiirtoResponse {
    override val result = TiedonsiirtoStatus.OK

    override fun toResponseEntity() = ResponseEntity(this, HttpStatus.OK)
}

data class TiedonsiirtoFailure(
    val statusCode: HttpStatusCode = HttpStatus.BAD_REQUEST,
    val errors: List<String>,
) : TiedonsiirtoResponse {
    override val result = TiedonsiirtoStatus.Failed

    override fun toResponseEntity() = ResponseEntity(this, statusCode)

    companion object {
        fun forbidden(msg: String) = TiedonsiirtoFailure(HttpStatus.FORBIDDEN, listOf(msg))

        fun notFound(msg: String) = TiedonsiirtoFailure(HttpStatus.NOT_FOUND, listOf(msg))

        fun conflict(msg: String) = TiedonsiirtoFailure(HttpStatus.CONFLICT, listOf(msg))

        fun badRequest(msg: String) = TiedonsiirtoFailure(HttpStatus.BAD_REQUEST, listOf(msg))

        fun badRequest(msgs: Iterable<String>) = TiedonsiirtoFailure(HttpStatus.BAD_REQUEST, msgs.toList())
    }
}
