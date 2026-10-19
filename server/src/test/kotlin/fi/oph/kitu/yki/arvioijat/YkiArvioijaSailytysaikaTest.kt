package fi.oph.kitu.yki.arvioijat

import fi.oph.kitu.DBContainerConfiguration
import fi.oph.kitu.TestTimeService
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Tutkintokieli
import fi.oph.kitu.yki.Tutkintotaso
import fi.oph.kitu.yki.arvioijat.Sailytysaika
import fi.oph.kitu.yki.arvioijat.YkiArvioijaEntity
import fi.oph.kitu.yki.arvioijat.YkiArvioijaRepository
import fi.oph.kitu.yki.arvioijat.YkiArvioijaService
import fi.oph.kitu.yki.arvioijat.YkiArvioijaTila
import fi.oph.kitu.yki.arvioijat.YkiArviointioikeusEntity
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@SpringBootTest
@Import(DBContainerConfiguration::class)
class YkiArvioijaSailytysaikaTest(
    @param:Autowired private val repository: YkiArvioijaRepository,
    @param:Autowired private val service: YkiArvioijaService,
    @param:Autowired private val timeService: TestTimeService,
) {
    private val hetki = Instant.parse("2026-06-01T09:00:00Z")
    private val tanaan = LocalDate.of(2026, 6, 1)
    private val umpeutunut = tanaan.minusYears(Sailytysaika.VUOSIA).minusDays(1)
    private val juuriJaAlle = tanaan.minusYears(Sailytysaika.VUOSIA).plusDays(1)

    @BeforeEach
    fun nukeDb() {
        repository.deleteAll()
    }

    private fun poista(): Int {
        var poistetut = 0
        timeService.runWithFixedClock(hetki) { poistetut = service.poistaSailytysajanYlittaneet() }
        return poistetut
    }

    @Test
    fun `umpeutuneen kauden merkinta poistetaan`() {
        val id = tallenna(kaudenPaattymispaiva = umpeutunut)

        assertEquals(1, poista())
        assertNull(repository.findArvioijaById(id), "merkinnan on poistuttava")
    }

    @Test
    fun `sailytysajan sisalla oleva merkinta sailyy`() {
        val id = tallenna(kaudenPaattymispaiva = juuriJaAlle)

        assertEquals(0, poista())
        assertNotNull(repository.findArvioijaById(id))
    }

    @Test
    fun `voimassa oleva arviointioikeus suojaa poistolta`() {
        val id =
            tallenna(
                kaudenPaattymispaiva = umpeutunut,
                toinenOikeus = tanaan.plusYears(1),
            )

        assertEquals(0, poista(), "yksikin voimassa oleva oikeus riittaa suojaksi")
        assertNotNull(repository.findArvioijaById(id))
    }

    @Test
    fun `merkinta jonka alkuhetkea ei tiedeta ei poistu koskaan`() {
        val id = tallenna(kaudenPaattymispaiva = null, tallennettuTila = YkiArvioijaTila.PASSIVOITU)

        assertEquals(0, poista(), "vanhinta ja epaluotettavinta dataa ei poisteta automaattisesti")
        assertNotNull(repository.findArvioijaById(id))
    }

    @Test
    fun `manuaalinen passivointihetki ratkaisee kauden paattymispaivan sijaan`() {
        // Kesken kauden passivoitu: sailytysaika alkaa passivointihetkesta, joka on aiempi.
        val id =
            tallenna(
                kaudenPaattymispaiva = juuriJaAlle,
                passivoitu = umpeutunut.atStartOfDay().atOffset(ZoneOffset.UTC),
            )

        assertEquals(1, poista())
        assertNull(repository.findArvioijaById(id))
    }

    @Test
    fun `poisto vie mukanaan arviointioikeudet ja kausihistorian`() {
        val id = tallenna(kaudenPaattymispaiva = umpeutunut)
        assertEquals(1, repository.findKausihistoria(id).size, "kausihistoria on kirjattu")

        poista()

        assertEquals(0, repository.findKausihistoria(id).size, "historian on poistuttava kaskadina")
    }

    private fun tallenna(
        kaudenPaattymispaiva: LocalDate?,
        toinenOikeus: LocalDate? = null,
        passivoitu: OffsetDateTime? = null,
        tallennettuTila: YkiArvioijaTila? = null,
    ): Int {
        val oikeudet =
            listOfNotNull(
                oikeus(Tutkintokieli.FIN, kaudenPaattymispaiva, tallennettuTila),
                toinenOikeus?.let { oikeus(Tutkintokieli.SWE, it, null) },
            )

        return repository.tallenna(
            YkiArvioijaEntity(
                id = null,
                arvioijaOid = Oid.parse("1.2.246.562.24.20281155246").getOrThrow(),
                henkilotunnus = null,
                sukunimi = "Testi",
                etunimet = "Testi",
                sahkopostiosoite = null,
                katuosoite = "Testikuja 5",
                postinumero = "40100",
                postitoimipaikka = "Testilä",
                passivoitu = passivoitu,
                arviointioikeudet = oikeudet,
            ),
        )
    }

    private fun oikeus(
        kieli: Tutkintokieli,
        paattyy: LocalDate?,
        tallennettuTila: YkiArvioijaTila?,
    ) = YkiArviointioikeusEntity(
        id = null,
        arvioijaId = null,
        kieli = kieli,
        tasot = setOf(Tutkintotaso.PT),
        tila = tallennettuTila,
        kaudenAlkupaiva = paattyy?.minusYears(5),
        kaudenPaattymispaiva = paattyy,
        jatkorekisterointi = false,
        ensimmainenRekisterointipaiva = LocalDate.of(2010, 1, 1),
        rekisteriintuontiaika = null,
    )
}
