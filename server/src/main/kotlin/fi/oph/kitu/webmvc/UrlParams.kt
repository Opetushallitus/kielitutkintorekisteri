package fi.oph.kitu.webmvc

fun Boolean?.toTrueOrNull(): String? = if (this == true) "true" else null

fun <E : Enum<E>> List<E>.toUrlParam(): String? = if (isEmpty()) null else sorted().joinToString(",") { it.name }
