package fi.oph.kitu.webmvc

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ResponseStatus

@ResponseStatus(HttpStatus.BAD_REQUEST)
open class VirheellinenPyyntoException(
    reason: String,
) : RuntimeException(reason)
