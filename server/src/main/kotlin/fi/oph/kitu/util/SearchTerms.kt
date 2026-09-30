package fi.oph.kitu.util

import arrow.core.NonEmptySet
import arrow.core.toNonEmptySetOrNull
import fi.oph.kitu.oid.Oid.Companion.isOidOfClass
import fi.oph.kitu.oid.OidClass

data class SearchTerms(
    val terms: Map<TermKind, NonEmptySet<String>>,
) {
    val allTerms: List<String> by lazy { terms.values.flatten() }

    fun henkiloOids(): NonEmptySet<String>? = terms[TermKind.HenkiloOid]

    fun withHenkiloOids(oids: NonEmptySet<String>): SearchTerms = copy(terms = terms + (TermKind.HenkiloOid to oids))

    fun orgOids(): NonEmptySet<String>? = terms[TermKind.OrgOid]

    fun numbers(): NonEmptySet<Int>? = terms[TermKind.Number]?.map { it.toInt() }?.toNonEmptySet()

    fun texts(): NonEmptySet<String>? = terms[TermKind.Text]

    companion object {
        fun from(query: String?): SearchTerms =
            SearchTerms(
                query
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.split(Regex("[\\s,;]+"))
                    .orEmpty()
                    .groupBy { TermKind.from(it) }
                    .mapValues { it.value.toNonEmptySetOrNull()!! },
            )

        enum class TermKind {
            HenkiloOid,
            OrgOid,
            Number,
            Text,
            ;

            companion object {
                fun from(term: String): TermKind =
                    when {
                        term.isOidOfClass(OidClass.OPPIJA) -> HenkiloOid
                        term.isOidOfClass(OidClass.ORG) -> OrgOid
                        term.toIntOrNull() != null -> Number
                        else -> Text
                    }
            }
        }
    }
}
