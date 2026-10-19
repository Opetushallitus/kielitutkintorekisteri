package fi.oph.kitu.jdbc

class SqlFilterBuilder {
    private val clauses = mutableListOf<String>()
    private val params = mutableMapOf<String, Any?>()

    fun add(
        condition: String?,
        vararg bindings: Pair<String, Any?>,
    ) {
        add(condition, bindings.toMap())
    }

    fun add(
        condition: String?,
        bindings: Map<String, Any?>,
    ) {
        if (condition != null) {
            val duplicates = bindings.keys intersect params.keys
            require(duplicates.isEmpty()) { "SQL-parametri annettu kahdesti: $duplicates" }
            clauses.add(condition)
            params.putAll(bindings)
        }
    }

    fun whereClauseOrNull(): String? =
        if (clauses.isEmpty()) null else "WHERE ${clauses.joinToString(" AND ") { "($it)" }}"

    fun params(): Map<String, Any?> = params.toMap()
}
