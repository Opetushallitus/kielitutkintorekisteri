package fi.oph.kitu.jdbc

interface ConflictHandler {
    val conflictTarget: ConflictTarget
}

data class UpdateOnConflict(
    override val conflictTarget: ConflictTarget,
    val columns: Iterable<String>,
) : ConflictHandler {
    override fun toString() =
        """
        ON CONFLICT $conflictTarget
        DO UPDATE SET
        ${columns.joinToString(",\n") { "$it = EXCLUDED.$it" }}
        """.trimIndent()
}

sealed interface ConflictTarget

data class Constraint(
    val name: String,
) : ConflictTarget {
    override fun toString() = "ON CONSTRAINT $name"
}

data class Columns(
    val names: Iterable<String>,
) : ConflictTarget {
    override fun toString() = names.joinToString(", ", "(", ")")

    companion object {
        fun of(vararg names: String) = Columns(names.toList())
    }
}
