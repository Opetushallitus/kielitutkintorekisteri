package fi.oph.kitu.tehtavapankki

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.util.defaultObjectMapper
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest
@Import(DBContainerConfiguration::class)
class TehtavapankkiRepositoryTest(
    @param:Autowired private val repo: TehtavapankkiRepository,
    @param:Autowired private val jdbc: JdbcTemplate,
) {
    @org.junit.jupiter.api.BeforeEach
    fun cleanTables() {
        // Cascade-deletoi kaikki testin aikana luodut paketit, tehtävät ja vastaukset.
        jdbc.execute("DELETE FROM tehtavapaketti")
    }

    private fun seedPaketti(
        lahdejarjestelma: String = "moodle.koealusta",
        lahdeId: String = "42",
        nimi: String = "Suomi alkeet",
        versioHash: String = "hash-v1",
        s3Avain: String? = "42-Suomi_alkeet/2026-01-01.xml",
    ): Int =
        repo.insertPaketti(
            TehtavapakettiEntity(
                lahdejarjestelma = lahdejarjestelma,
                lahdeId = lahdeId,
                nimi = nimi,
                versioHash = versioHash,
                s3Avain = s3Avain,
            ),
        )

    private fun seedRyhma(
        pakettiId: Int,
        nimi: String = "Yleinen",
        jarjestys: Int = 1,
    ): Int =
        repo
            .insertRyhmat(
                listOf(
                    TehtavaryhmaEntity(pakettiId = pakettiId, nimi = nimi, jarjestys = jarjestys),
                ),
            ).single()

    @Test
    fun `paketti, tehtavat ja vastaukset tallentuvat ja luetaan jarjestyksessa`() {
        val pakettiId = seedPaketti()
        val ryhmaId = seedRyhma(pakettiId)

        val tehtavaIds =
            repo.insertTehtavat(
                listOf(
                    TehtavaEntity(
                        pakettiId = pakettiId,
                        ryhmaId = ryhmaId,
                        tyyppi = "multichoice",
                        nimi = "Eka kysymys",
                        teksti = "<p>Mikä?</p>",
                        tekstinFormaatti = "html",
                        jarjestys = 1,
                        metadata =
                            defaultObjectMapper.readTree(
                                """{"defaultgrade": "1.0000000", "penalty": "0.3333333"}""",
                            ),
                    ),
                    TehtavaEntity(
                        pakettiId = pakettiId,
                        ryhmaId = ryhmaId,
                        tyyppi = "shortanswer",
                        nimi = "Toka kysymys",
                        teksti = "Vastaa.",
                        tekstinFormaatti = "html",
                        jarjestys = 2,
                    ),
                ),
            )

        val correct = defaultObjectMapper.readTree("""{"fraction": "100"}""")
        val wrong = defaultObjectMapper.readTree("""{"fraction": "0"}""")
        repo.insertVastaukset(
            listOf(
                TehtavaVastausEntity(tehtavaId = tehtavaIds[0], jarjestys = 1, teksti = "A", metadata = correct),
                TehtavaVastausEntity(tehtavaId = tehtavaIds[0], jarjestys = 2, teksti = "B", metadata = wrong),
                TehtavaVastausEntity(
                    tehtavaId = tehtavaIds[1],
                    jarjestys = 1,
                    teksti = "020 123 456",
                    metadata = correct,
                ),
                TehtavaVastausEntity(
                    tehtavaId = tehtavaIds[1],
                    jarjestys = 2,
                    teksti = "020123456",
                    metadata = correct,
                ),
            ),
        )

        val paketti = repo.findPakettiById(pakettiId)
        assertNotNull(paketti)
        assertEquals("Suomi alkeet", paketti.nimi)
        assertEquals("42-Suomi_alkeet/2026-01-01.xml", paketti.s3Avain)
        assertNotNull(paketti.luotu)

        val tehtavat = repo.findTehtavatByPakettiId(pakettiId)
        assertEquals(2, tehtavat.size)
        assertEquals(listOf(1, 2), tehtavat.map { it.jarjestys })
        assertEquals("multichoice", tehtavat[0].tyyppi)
        assertEquals("1.0000000", tehtavat[0].metadata.get("defaultgrade").asString())
        assertEquals("0.3333333", tehtavat[0].metadata.get("penalty").asString())

        val vastauksetByTehtava = repo.findVastauksetByTehtavaIds(tehtavaIds)
        assertEquals(setOf(tehtavaIds[0], tehtavaIds[1]), vastauksetByTehtava.keys)
        assertEquals(listOf("A", "B"), vastauksetByTehtava[tehtavaIds[0]]!!.map { it.teksti })
        assertEquals(
            listOf("020 123 456", "020123456"),
            vastauksetByTehtava[tehtavaIds[1]]!!.map { it.teksti },
        )
    }

    @Test
    fun `tehtavan tiedostot tallennetaan ja luetaan ryhmiteltyna tehtava-id n mukaan`() {
        val pakettiId = seedPaketti()
        val ryhmaId = seedRyhma(pakettiId)
        val tehtavaIds =
            repo.insertTehtavat(
                listOf(
                    TehtavaEntity(pakettiId = pakettiId, ryhmaId = ryhmaId, tyyppi = "description", jarjestys = 1),
                    TehtavaEntity(pakettiId = pakettiId, ryhmaId = ryhmaId, tyyppi = "description", jarjestys = 2),
                ),
            )
        repo.insertTiedostot(
            listOf(
                TehtavaTiedostoEntity(
                    tehtavaId = tehtavaIds[0],
                    tiedostonimi = "audio.mp3",
                    s3Avain = "42-Suomi/2026-01-01 assets/audio.mp3",
                ),
                TehtavaTiedostoEntity(
                    tehtavaId = tehtavaIds[0],
                    tiedostonimi = "image.png",
                    s3Avain = "42-Suomi/2026-01-01 assets/image.png",
                ),
                TehtavaTiedostoEntity(
                    tehtavaId = tehtavaIds[1],
                    tiedostonimi = "kuva.png",
                    s3Avain = "42-Suomi/2026-01-01 assets/kuva.png",
                ),
            ),
        )

        val byTehtava = repo.findTiedostotByTehtavaIds(tehtavaIds)
        assertEquals(setOf(tehtavaIds[0], tehtavaIds[1]), byTehtava.keys)
        assertEquals(
            listOf("audio.mp3", "image.png"),
            byTehtava[tehtavaIds[0]]!!.map { it.tiedostonimi },
        )
        assertEquals(
            "42-Suomi/2026-01-01 assets/kuva.png",
            byTehtava[tehtavaIds[1]]!!.single().s3Avain,
        )
    }

    @Test
    fun `eri versio_hashit samalle lahde-id-parille rinnakkain, sama hash kahdesti ei`() {
        seedPaketti(versioHash = "hash-v1")
        Thread.sleep(20) // varmistaa eri 'luotu'-aikaleimat
        val v2 = seedPaketti(versioHash = "hash-v2")

        assertTrue(repo.existsByVersionHash("moodle.koealusta", "42", "hash-v1"))
        assertTrue(repo.existsByVersionHash("moodle.koealusta", "42", "hash-v2"))
        assertFalse(repo.existsByVersionHash("moodle.koealusta", "42", "hash-v3"))

        val latest = repo.findLatestPakettiBySource("moodle.koealusta", "42")
        assertNotNull(latest)
        assertEquals(v2, latest.id)
        assertEquals("hash-v2", latest.versioHash)

        assertFailsWith<DuplicateKeyException> {
            seedPaketti(versioHash = "hash-v1") // sama (lahde,id,hash) -> UNIQUE rikkoutuu
        }
    }

    @Test
    fun `metadata-jsonb pyorahtaa tallennuksen ja luvun lapi`() {
        val pakettiId =
            repo.insertPaketti(
                TehtavapakettiEntity(
                    lahdejarjestelma = "moodle.koealusta",
                    lahdeId = "42",
                    nimi = "x",
                    versioHash = "h",
                    metadata = defaultObjectMapper.readTree("""{"courseid": 42, "fetchedAt": "2026-01-01"}"""),
                ),
            )
        val ryhmaId = seedRyhma(pakettiId)
        val tehtavaIds =
            repo.insertTehtavat(
                listOf(
                    TehtavaEntity(
                        pakettiId = pakettiId,
                        ryhmaId = ryhmaId,
                        tyyppi = "multichoice",
                        jarjestys = 1,
                        metadata =
                            defaultObjectMapper.readTree(
                                """{"single": true, "shuffleanswers": true, "answernumbering": "abc"}""",
                            ),
                    ),
                ),
            )

        val paketti = repo.findPakettiById(pakettiId)!!
        assertEquals(42, paketti.metadata.get("courseid").asInt())
        assertEquals("2026-01-01", paketti.metadata.get("fetchedAt").asString())

        val tehtava = repo.findTehtavatByPakettiId(pakettiId).single()
        assertEquals(true, tehtava.metadata.get("single").asBoolean())
        assertEquals("abc", tehtava.metadata.get("answernumbering").asString())
        assertEquals(tehtavaIds[0], tehtava.id)
    }

    @Test
    fun `findPaketitByS3Avaimet palauttaa loytyneet paketit ja jattaa tuntemattomat pois`() {
        val pakettiIdA = seedPaketti(lahdeId = "1", versioHash = "h1", s3Avain = "1-foo/a.xml")
        val pakettiIdB = seedPaketti(lahdeId = "2", versioHash = "h2", s3Avain = "2-bar/b.xml")

        val paketit =
            repo.findPaketitByS3Avaimet(
                listOf("1-foo/a.xml", "2-bar/b.xml", "ei-loydy/c.xml"),
            )

        assertEquals(setOf("1-foo/a.xml", "2-bar/b.xml"), paketit.map { it.s3Avain }.toSet())
        assertEquals(pakettiIdA, paketit.single { it.s3Avain == "1-foo/a.xml" }.id)
        assertEquals(pakettiIdB, paketit.single { it.s3Avain == "2-bar/b.xml" }.id)
    }

    @Test
    fun `tyyppi on TEXT-kentta ilman enum-rajoitusta - vapaa lahdetyyppi tallentuu sellaisenaan`() {
        val pakettiId = seedPaketti()
        val ryhmaId = seedRyhma(pakettiId)
        val tehtavaIds =
            repo.insertTehtavat(
                listOf(
                    TehtavaEntity(
                        pakettiId = pakettiId,
                        ryhmaId = ryhmaId,
                        tyyppi = "made_up_type_for_test",
                        jarjestys = 1,
                    ),
                ),
            )

        val tehtava = repo.findTehtavatByPakettiId(pakettiId).single()
        assertEquals("made_up_type_for_test", tehtava.tyyppi)
        assertEquals(tehtavaIds[0], tehtava.id)
    }
}
