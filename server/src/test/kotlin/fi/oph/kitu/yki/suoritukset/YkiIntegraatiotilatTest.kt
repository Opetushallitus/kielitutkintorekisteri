package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.dev.mockdata.generateRandomYkiSuoritusEntity
import fi.oph.kitu.i18n.LocalizedString
import fi.oph.kitu.i18n.UiText
import fi.oph.kitu.koodisto.Koodisto
import fi.oph.kitu.koski.KoskiErrorEntity
import fi.oph.kitu.oid.Oid
import fi.oph.kitu.util.result.getOrThrow
import fi.oph.kitu.yki.Arviointitila
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNull

class YkiIntegraatiotilatTest {
    private val tallennettu = Instant.parse("2026-11-10T10:00:00Z")

    private val arvioitu =
        generateRandomYkiSuoritusEntity().copy(
            suorittajanOID = Oid.parse("1.2.246.562.24.00000000001").getOrThrow(),
            arviointitila = Arviointitila.ARVIOITU,
            tekstinYmmartaminen = 3,
            kirjoittaminen = 3,
            rakenteetJaSanasto = null,
            puheenYmmartaminen = 3,
            puhuminen = 3,
            yleisarvosana = null,
            lastModified = tallennettu,
            receivedAt = tallennettu,
            koskiSiirtoKasitelty = false,
            koskiSiirretty = null,
            arviointitilaLahetetty = null,
            arviointitilanLahetysvirhe = null,
            arviointitilanLahetysyritys = null,
        )

    private fun koskiError(aika: Instant) =
        KoskiErrorEntity(id = "1", entity = "yki", message = "virhe", timestamp = aika, hidden = false)

    private fun laske(
        suoritus: YkiSuoritusEntity = arvioitu,
        viimeisin: YkiSuoritusEntity = suoritus,
        koskiError: KoskiErrorEntity? = null,
        estonSyyt: List<String>? = null,
    ) = YkiIntegraatiotilat.laske(suoritus, viimeisin, koskiError, estonSyyt)

    @Test
    fun `solki on aina onnistunut ja kertoo rekisteriintuontiajan`() {
        val solki = laske().solki
        assertEquals(IntegraationTila.ONNISTUI, solki.tila)
        assertEquals(Aikaleima(UiText.Yki.Sarake.rekisteriintuontiaika, tallennettu), solki.aikaleima)
    }

    @Test
    fun `koski odottaa kun siirtoa ei ole kasitelty`() {
        assertEquals(IntegraationTila.ODOTTAA, laske().koski.tila)
    }

    @Test
    fun `koski onnistunut siirtoajan kanssa`() {
        val siirretty = Instant.parse("2026-11-10T10:15:00Z")
        val koski = laske(arvioitu.copy(koskiSiirtoKasitelty = true, koskiSiirretty = siirretty)).koski
        assertEquals(IntegraationTila.ONNISTUI, koski.tila)
        assertEquals(Aikaleima(null, siirretty), koski.aikaleima)
    }

    @Test
    fun `koski onnistunut ilman siirtoaikaa ennen aikaleiman tallennusta`() {
        val koski = laske(arvioitu.copy(koskiSiirtoKasitelty = true, koskiSiirretty = null)).koski
        assertEquals(IntegraationTila.ONNISTUI, koski.tila)
        assertEquals(Aikaleima(null, null), koski.aikaleima)
    }

    @Test
    fun `koski epaonnistunut vaikka aiempi siirto onnistui`() {
        val virheaika = Instant.parse("2026-11-11T08:00:00Z")
        val koski =
            laske(
                arvioitu.copy(koskiSiirtoKasitelty = true, koskiSiirretty = tallennettu),
                koskiError = koskiError(virheaika),
            ).koski
        assertEquals(IntegraationTila.EPAONNISTUI, koski.tila)
        assertEquals(Aikaleima(UiText.Yki.Integraatio.viimeisinYritys, virheaika), koski.aikaleima)
    }

    @Test
    fun `koski ei siirreta luettelee kaikki estosyyt`() {
        val koski =
            laske(
                arvioitu.copy(koskiSiirtoKasitelty = true),
                koskiError = koskiError(tallennettu),
                estonSyyt = listOf("Syy 1", "Syy 2"),
            ).koski
        assertEquals(IntegraationTila.EI_SIIRRETA, koski.tila)
        assertEquals(listOf(LocalizedString(fi = "Syy 1"), LocalizedString(fi = "Syy 2")), koski.selitteet)
    }

    @Test
    fun `kios ei laheteta ilmoittautumiselle, vilpille tai ilman oppijanumeroa`() {
        val vilppi =
            Koodisto.YkiArvosana.Vilppi.koodiarvo
                .toInt()
        listOf(
            arvioitu.copy(arviointitila = Arviointitila.ILMOITTAUTUNUT) to UiText.Yki.suoritustaEdeltavaEiLaheteta,
            arvioitu.copy(puhuminen = vilppi) to UiText.Yki.Integraatio.vilppiaEiLaheteta,
            arvioitu.copy(suorittajanOID = null) to UiText.Yki.eiOppijanumeroa,
        ).forEach { (suoritus, syy) ->
            val kios = laske(suoritus).kios
            assertEquals(IntegraationTila.EI_SIIRRETA, kios.tila)
            assertEquals(listOf(syy), kios.selitteet)
        }
    }

    @Test
    fun `kios onnistunut kun lahetys on viimeisimman version jalkeen`() {
        val lahetetty = Instant.parse("2026-11-10T10:00:01Z")
        val kios = laske(arvioitu.copy(arviointitilaLahetetty = Timestamp.from(lahetetty))).kios
        assertEquals(IntegraationTila.ONNISTUI, kios.tila)
        assertEquals(Aikaleima(null, lahetetty), kios.aikaleima)
    }

    @Test
    fun `kios odottaa uutta versiota ja nayttaa edellisen lahetyksen`() {
        val lahetetty = Instant.parse("2026-11-09T10:00:00Z")
        val vanha = arvioitu.copy(id = 1, arviointitilaLahetetty = Timestamp.from(lahetetty))
        val uusi = vanha.copy(id = 2, lastModified = tallennettu.plusSeconds(3600))
        val kios = laske(vanha, viimeisin = uusi).kios
        assertEquals(IntegraationTila.ODOTTAA, kios.tila)
        assertEquals(Aikaleima(UiText.Yki.Integraatio.edellinenLahetys, lahetetty), kios.aikaleima)
    }

    @Test
    fun `kios odottaa ilman aikaleimaa kun lahetysta ei ole tehty`() {
        val kios = laske().kios
        assertEquals(IntegraationTila.ODOTTAA, kios.tila)
        assertNull(kios.aikaleima)
    }

    @Test
    fun `kios epaonnistunut viimeisimman yrityksen ajan kanssa`() {
        val yritys = Instant.parse("2026-11-10T11:00:00Z")
        val kios =
            laske(arvioitu.copy(arviointitilanLahetysvirhe = "500", arviointitilanLahetysyritys = yritys)).kios
        assertEquals(IntegraationTila.EPAONNISTUI, kios.tila)
        assertEquals(Aikaleima(UiText.Yki.Integraatio.viimeisinYritys, yritys), kios.aikaleima)
    }
}
