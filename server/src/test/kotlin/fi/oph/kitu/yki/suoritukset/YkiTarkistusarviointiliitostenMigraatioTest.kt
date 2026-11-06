package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.tyhjennaYkiSuoritukset
import fi.oph.kitu.yki.TutkinnonOsa
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertEquals

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiTarkistusarviointiliitostenMigraatioTest(
    @param:Autowired private val jdbc: JdbcTemplate,
    @param:Autowired private val suoritusRepository: YkiSuoritusRepository,
) {
    @BeforeEach
    fun tyhjennaTaulut() {
        jdbc.tyhjennaYkiSuoritukset()
        jdbc.execute("TRUNCATE TABLE yki_tarkistusarviointi CASCADE")
    }

    @AfterEach
    fun palautaUniikkiusehto() {
        ajaMigraatio()
    }

    @Test
    fun `kopiot yhdistetaan yhdeksi liitokseksi eika luettu suoritus muutu`() {
        val suoritus = tallennaTarkistusarvioitu(arvosanaMuuttui = setOf(TutkinnonOsa.puhuminen))
        val ennen = suoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).single()
        poistaUniikkiusehto()
        kopioiLiitokset(arvosanaMuuttuiLauseke = "arvosana_muuttui")
        kopioiLiitokset(arvosanaMuuttuiLauseke = "NULL")

        ajaMigraatio()

        assertEquals(listOf("KI" to false, "PU" to true), liitokset())
        assertEquals(ennen, suoritusRepository.findLatestBySolkiIds(listOf(suoritus.solkiId)).single())
    }

    @Test
    fun `false sailyy kun Koski-siirron kopio on NULL`() {
        tallennaTarkistusarvioitu(arvosanaMuuttui = null)
        jdbc.update("UPDATE yki_osakoe_tarkistusarviointi SET arvosana_muuttui = false")
        poistaUniikkiusehto()
        kopioiLiitokset(arvosanaMuuttuiLauseke = "NULL")

        ajaMigraatio()

        assertEquals(listOf("KI" to false, "PU" to false), liitokset())
    }

    @Test
    fun `uudelleenajo on idempotentti ja uniikkiusehto estaa uudet kopiot`() {
        tallennaTarkistusarvioitu(arvosanaMuuttui = setOf(TutkinnonOsa.puhuminen))
        poistaUniikkiusehto()
        kopioiLiitokset(arvosanaMuuttuiLauseke = "arvosana_muuttui")

        ajaMigraatio()
        ajaMigraatio()

        assertEquals(listOf("KI" to false, "PU" to true), liitokset())
        assertThrows<DuplicateKeyException> { kopioiLiitokset(arvosanaMuuttuiLauseke = "arvosana_muuttui") }
    }

    private fun tallennaTarkistusarvioitu(arvosanaMuuttui: Set<TutkinnonOsa>?): YkiSuoritusEntity =
        generateRandomYkiSuoritusEntity()
            .copy(
                tarkistusarvioidutOsakokeet = setOf(TutkinnonOsa.puhuminen, TutkinnonOsa.kirjoittaminen),
                arvosanaMuuttui = arvosanaMuuttui,
            ).also { suoritusRepository.save(it, false) }

    private fun poistaUniikkiusehto() {
        jdbc.execute(
            "ALTER TABLE yki_osakoe_tarkistusarviointi DROP CONSTRAINT IF EXISTS yki_osakoe_tarkistusarviointi_unique",
        )
    }

    private fun kopioiLiitokset(arvosanaMuuttuiLauseke: String) {
        jdbc.update(
            """
            INSERT INTO yki_osakoe_tarkistusarviointi (osakoe_id, tarkistusarviointi_id, arvosana_muuttui)
            SELECT osakoe_id, tarkistusarviointi_id, $arvosanaMuuttuiLauseke
            FROM yki_osakoe_tarkistusarviointi
            """.trimIndent(),
        )
    }

    private fun liitokset(): List<Pair<String, Boolean?>> =
        jdbc.query(
            """
            SELECT o.tyyppi::text, l.arvosana_muuttui
            FROM yki_osakoe_tarkistusarviointi l
                JOIN yki_osakoe o ON o.id = l.osakoe_id
            ORDER BY 1
            """.trimIndent(),
        ) { rs, _ -> rs.getString(1) to rs.getObject(2) as Boolean? }

    private fun ajaMigraatio() {
        val sql =
            ClassPathResource("db/migration/V140__yhdista_tarkistusarviointiliitosten_kopiot.sql")
                .inputStream
                .bufferedReader()
                .use { it.readText() }
        jdbc.execute(sql)
    }
}
