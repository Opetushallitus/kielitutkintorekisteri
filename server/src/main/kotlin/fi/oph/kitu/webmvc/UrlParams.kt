package fi.oph.kitu.webmvc

fun Boolean?.toTrueOrNull(): String? = if (this == true) "true" else null
