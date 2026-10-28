package fi.oph.kitu.jdbc

abstract class SqlFilter {
    protected abstract fun SqlFilterBuilder.build()

    private val spec by lazy { SqlFilterBuilder().apply { build() } }

    fun whereSql(): String? = spec.whereClauseOrNull()

    fun params(): Map<String, Any?> = spec.params()
}

data class SqlCondition(
    val sql: String,
    val params: Map<String, Any?>,
)

/**
 * Jokaisen hakusanan on osuttava johonkin sarakkeista, jolloin "Ranja Öhman" löytää henkilön,
 * vaikka termit ovat eri sarakkeissa.
 */
fun searchTermsCondition(
    terms: Collection<String>,
    columns: List<String>,
    paramPrefix: String,
): SqlCondition? {
    if (terms.isEmpty()) return null
    val params = terms.withIndex().associate { (i, term) -> "${paramPrefix}_$i" to "%$term%" }
    val sql =
        params.keys.joinToString(" AND ") { param ->
            columns.joinToString(" OR ", prefix = "(", postfix = ")") { "$it ILIKE :$param" }
        }
    return SqlCondition(sql, params)
}

fun SqlFilterBuilder.addSearchTerms(
    terms: Collection<String>,
    columns: List<String>,
    paramPrefix: String,
) {
    searchTermsCondition(terms, columns, paramPrefix)?.let { add(it.sql, it.params) }
}

fun SqlFilterBuilder.addAnyOf(
    column: String,
    param: String,
    values: Collection<Enum<*>>,
) {
    if (values.isNotEmpty()) add("$column IN (:$param)", param to values.map { it.name })
}
