package fi.oph.kitu.jdbc

import fi.oph.kitu.util.searchTerms
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class SqlFilterTest {
    @Test
    fun `jokaisen hakusanan on osuttava johonkin sarakkeista`() {
        val ehto = searchTermsCondition(listOf("ranja", "öhman"), listOf("etunimet", "sukunimi"), "haku")!!

        assertEquals(
            "(etunimet ILIKE :haku_0 OR sukunimi ILIKE :haku_0) AND (etunimet ILIKE :haku_1 OR sukunimi ILIKE :haku_1)",
            ehto.sql,
        )
        assertEquals(mapOf("haku_0" to "%ranja%", "haku_1" to "%öhman%"), ehto.params)
    }

    @Test
    fun `ilman hakusanoja ei synny ehtoa`() {
        assertNull(searchTermsCondition(emptyList(), listOf("etunimet"), "haku"))
        assertNull(
            SqlFilterBuilder().apply { addSearchTerms(emptyList(), listOf("etunimet"), "haku") }.whereClauseOrNull(),
        )
    }

    @Test
    fun `yhdistetty haku etsii jokaista hakusanaa samasta lausekkeesta`() {
        val ehto =
            combinedSearchTermsCondition(listOf("ranja", "öhman"), listOf("etunimet", "sukunimi"), "fi_FI", "haku")!!

        val lauseke = "lower(concat_ws(' ', etunimet, sukunimi) COLLATE \"fi_FI\")"
        assertEquals(
            "$lauseke LIKE lower(:haku_0 COLLATE \"fi_FI\") AND $lauseke LIKE lower(:haku_1 COLLATE \"fi_FI\")",
            ehto.sql,
        )
        assertEquals(mapOf("haku_0" to "%ranja%", "haku_1" to "%öhman%"), ehto.params)
    }

    @Test
    fun `yhdistetty haku ilman hakusanoja ei tuota ehtoa`() {
        assertNull(combinedSearchTermsCondition(emptyList(), listOf("etunimet"), "fi_FI", "haku"))
        assertNull(
            SqlFilterBuilder()
                .apply { addCombinedSearchTerms(emptyList(), listOf("etunimet"), "fi_FI", "haku") }
                .whereClauseOrNull(),
        )
    }

    @Test
    fun `hakusanat erotellaan valilyonneilla, pilkuilla ja puolipisteilla`() {
        assertEquals(listOf("ranja", "öhman", "1.2.3"), searchTerms("  ranja,  öhman;1.2.3 "))
        assertEquals(emptyList(), searchTerms("  "))
        assertEquals(emptyList(), searchTerms(null))
    }

    @Test
    fun `sama parametri kahdesti on virhe`() {
        val builder = SqlFilterBuilder().apply { add("a = :p", "p" to 1) }

        assertFailsWith<IllegalArgumentException> { builder.add("b = :p", "p" to 2) }
    }
}
