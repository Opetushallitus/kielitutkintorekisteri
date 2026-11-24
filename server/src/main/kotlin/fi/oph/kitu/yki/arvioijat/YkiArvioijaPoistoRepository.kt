package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.oid.Oid
import fi.oph.kitu.oid.getOid
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.time.OffsetDateTime

data class YkiArvioijaPoisto(
    val id: Int,
    val arvioijaOid: Oid,
    val poistettu: OffsetDateTime,
)

/**
 * Solkiin lahetettavien poistoilmoitusten jono. Poistetulla arvioijalla ei ole enaa
 * `yki_arvioija`-rivia, joka muuten toimii lahetysjonona, joten ilmoitus odottaa taalla. Rivit
 * kirjaa [YkiArvioijaRepository.poistaSailytysajanYlittaneet].
 */
@Repository
class YkiArvioijaPoistoRepository(
    private val jdbcTemplate: JdbcTemplate,
) {
    fun findLahetettavat(
        maxYritykset: Int? = null,
        limit: Int = LAHETYSERAN_KOKO,
    ): List<YkiArvioijaPoisto> =
        jdbcTemplate.query(
            """
            SELECT id, arvioija_oid, poistettu
            FROM yki_arvioija_poisto
            WHERE ?::int IS NULL OR solki_lahetysyritykset < ?::int
            ORDER BY poistettu
            LIMIT ?
            """.trimIndent(),
            { rs, _ ->
                YkiArvioijaPoisto(
                    id = rs.getInt("id"),
                    arvioijaOid = rs.getOid("arvioija_oid"),
                    poistettu = rs.getObject("poistettu", OffsetDateTime::class.java),
                )
            },
            maxYritykset,
            maxYritykset,
            limit,
        )

    fun merkitseLahetetyksi(id: Int) {
        jdbcTemplate.update("DELETE FROM yki_arvioija_poisto WHERE id = ?", id)
    }

    fun merkitseLahetysvirhe(
        id: Int,
        virhe: String,
    ) {
        jdbcTemplate.update(
            """
            UPDATE yki_arvioija_poisto
            SET solki_lahetysvirhe = ?,
                solki_lahetysyritykset = solki_lahetysyritykset + 1,
                solki_viimeisin_lahetysyritys = now()
            WHERE id = ?
            """.trimIndent(),
            virhe,
            id,
        )
    }

    /**
     * Sama henkilo on voitu lisata rekisteriin uudelleen ennen kuin poistoilmoitus ehti lahtea.
     * Myohassa lahteva ilmoitus poistaisi silloin Solkista voimassa olevan arvioijan.
     */
    fun poistaUudelleenLisattyjenIlmoitukset(): Int =
        jdbcTemplate.update(
            """
            DELETE FROM yki_arvioija_poisto
            WHERE EXISTS (
                SELECT 1 FROM yki_arvioija WHERE yki_arvioija.arvioija_oid = yki_arvioija_poisto.arvioija_oid
            )
            """.trimIndent(),
        )

    companion object {
        private const val LAHETYSERAN_KOKO = 100
    }
}
